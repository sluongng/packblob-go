package packblob_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/klauspost/compress/zstd"
	"github.com/sluongng/packblob-go"
	packblobpb "github.com/sluongng/packblob-go/protobuf"
	packblobthrift "github.com/sluongng/packblob-go/thrift"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestIndependentEnvelopes(t *testing.T) {
	for _, encoding := range []struct {
		name   string
		opts   []packblob.Option
		header uint32
	}{
		{name: "default protobuf", header: 1},
		{name: "explicit thrift", opts: []packblob.Option{packblob.WithEncoding(packblob.Thrift)}},
	} {
		t.Run(encoding.name, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				input      []byte
				compressed bool
			}{
				{name: "empty", input: nil},
				{name: "small", input: []byte("hello")},
				{name: "compressible", input: bytes.Repeat([]byte("hello"), 4096), compressed: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					encoded, err := packblob.EncodeSingle(tc.input, 0, encoding.opts...)
					must(t, err)
					if binary.BigEndian.Uint32(encoded[:4]) != encoding.header {
						t.Fatal("EncodeSingle lost selected encoding")
					}
					storage := readEnvelope(t, encoded)[1].(fields)
					single := storage[1].(fields)
					if len(storage) != 1 || len(single) != 1 {
						t.Fatal("expected single union variants")
					}
					var actual []byte
					if tc.compressed {
						actual = decompress(t, single[2].([]byte), nil, false)
					} else {
						actual = single[1].([]byte)
					}
					if !bytes.Equal(actual, tc.input) {
						t.Fatal("decoded contents differ")
					}
				})
			}
		})
	}
	// Hand-auditable goldens check framing and Raw independently of readers.
	for _, tc := range []struct {
		encoding packblob.Encoding
		want     string
	}{
		{packblob.Protobuf, "000000010a090a070a0568656c6c6f"},
		{packblob.Thrift, "000000001c1c180568656c6c6f000000"},
	} {
		encoded, err := packblob.EncodeRaw([]byte("hello"), packblob.WithEncoding(tc.encoding))
		must(t, err)
		if got := hex.EncodeToString(encoded); got != tc.want {
			t.Fatalf("raw envelope encoding %d = %s, want %s", tc.encoding, got, tc.want)
		}
	}
}

func TestPack(t *testing.T) {
	for _, encoding := range []struct {
		name   string
		value  packblob.Encoding
		suffix string
	}{
		{"protobuf", packblob.Protobuf, packblob.EnvelopeSuffix},
		{"thrift", packblob.Thrift, packblob.ThriftEnvelopeSuffix},
	} {
		t.Run(encoding.name, func(t *testing.T) {
			builder, err := packblob.NewBuilder(0, packblob.WithEncoding(encoding.value))
			must(t, err)
			inputs := make(map[string][]byte)
			for i := range 20 {
				key := fmt.Sprint(i)
				data := bytes.Repeat([]byte(fmt.Sprintf("revision %d\n", i)), 1024)
				inputs[key] = data
				if i == 0 {
					must(t, builder.AddBase(key, data))
				} else {
					must(t, builder.AddDelta(fmt.Sprint(i-1), key, data))
				}
			}
			blob, err := builder.Build("")
			must(t, err)
			// This is the key asserted by Mononoke's pack_zstd_from_dict_unchained_test
			// for logical keys "0" through "19", independent of contents and ordering.
			wantKey := "49df56ffd39791720f4d4b5c05d3156dfeab994fcb6b0000b3c2343280b95975" + encoding.suffix
			if blob.Encoding != encoding.value {
				t.Fatalf("Blob.Encoding = %d, want %d", blob.Encoding, encoding.value)
			}
			if blob.Key != wantKey || !slices.IsSorted(blob.LinkKeys) || len(blob.LinkKeys) != 20 {
				t.Fatalf("unexpected pack identity or links: %q, %v", blob.Key, blob.LinkKeys)
			}
			storage := readEnvelope(t, blob.Data)[1].(fields)
			pack := storage[2].(fields)
			if string(pack[1].([]byte)) != blob.Key {
				t.Fatal("embedded pack key differs")
			}
			entries := pack[2].([]fields)
			if len(entries) != len(inputs) {
				t.Fatalf("entry count = %d", len(entries))
			}
			decoded := make(map[string][]byte)
			for i, entry := range entries {
				key := string(entry[1].([]byte))
				if key != fmt.Sprint(i) {
					t.Fatalf("entry %d key = %q; insertion order lost", i, key)
				}
				value := entry[2].(fields)
				var actual []byte
				if i == 0 {
					actual = decompress(t, value[1].(fields)[2].([]byte), nil, false)
				} else {
					delta := value[2].(fields)
					dictKey := string(delta[1].([]byte))
					if dictKey != fmt.Sprint(i-1) {
						t.Fatalf("wrong dictionary for %q: %q", key, dictKey)
					}
					actual = decompress(t, delta[2].([]byte), decoded[dictKey], true)
				}
				if !bytes.Equal(actual, inputs[key]) {
					t.Fatalf("decoded contents differ for %q", key)
				}
				decoded[key] = actual
			}

		})
	}
}

func TestBuilderErrors(t *testing.T) {
	b, err := packblob.NewBuilder(3)
	must(t, err)
	if _, err := b.Build(""); !errors.Is(err, packblob.ErrEmptyPack) {
		t.Fatalf("empty Build error = %v", err)
	}
	must(t, b.AddBase("base", []byte("data")))
	before, err := b.Build("")
	must(t, err)
	for _, tc := range []struct {
		name string
		add  func() error
		want error
	}{
		{name: "duplicate base", add: func() error { return b.AddBase("base", nil) }, want: packblob.ErrDuplicateKey},
		{name: "duplicate delta", add: func() error { return b.AddDelta("base", "base", nil) }, want: packblob.ErrDuplicateKey},
		{name: "forward dictionary", add: func() error { return b.AddDelta("later", "new", nil) }, want: packblob.ErrMissingDictionary},
		{name: "repository prefix", add: func() error { return b.AddBase("repo0001.value", nil) }, want: packblob.ErrInvalidKey},
		{name: "ephemeral prefix", add: func() error { return b.AddBase("eph42.repo0001.value", nil) }, want: packblob.ErrInvalidKey},
		{name: "unicode repository prefix", add: func() error { return b.AddBase("repo１２３４.value", nil) }, want: packblob.ErrInvalidKey},
		{name: "invalid UTF-8", add: func() error { return b.AddBase("\xff", nil) }, want: packblob.ErrInvalidKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.add(); !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			after, err := b.Build("")
			must(t, err)
			if !bytes.Equal(before.Data, after.Data) {
				t.Fatal("failed add changed pack")
			}
		})
	}
	if _, err := b.Build("\xff"); !errors.Is(err, packblob.ErrInvalidKey) {
		t.Fatalf("invalid prefix error = %v", err)
	}
	for _, level := range []int{-131073, 23} {
		if _, err := packblob.NewBuilder(level); !errors.Is(err, packblob.ErrInvalidLevel) {
			t.Fatalf("NewBuilder(%d) error = %v", level, err)
		}
		if _, err := packblob.EncodeSingle(nil, level); !errors.Is(err, packblob.ErrInvalidLevel) {
			t.Fatalf("EncodeSingle level %d error = %v", level, err)
		}
	}
}

func TestBufferOwnershipAndRepeatedBuild(t *testing.T) {
	data := bytes.Repeat([]byte("dictionary contents"), 1024)
	original := bytes.Clone(data)
	var b packblob.Builder
	must(t, b.AddBase("base", data))
	clear(data) // The builder must not retain this caller-owned buffer.
	must(t, b.AddDelta("base", "child", original))
	first, err := b.Build("packs/")
	must(t, err)
	saved := bytes.Clone(first.Data)
	first.LinkKeys[0] = "changed"
	clear(first.Data)
	second, err := b.Build("packs/")
	must(t, err)
	if !bytes.Equal(second.Data, saved) || second.LinkKeys[0] != "base" {
		t.Fatal("Build returned shared output buffers")
	}
	pack := readEnvelope(t, second.Data)[1].(fields)[2].(fields)
	delta := pack[2].([]fields)[1][2].(fields)[2].(fields)
	if actual := decompress(t, delta[2].([]byte), original, true); !bytes.Equal(actual, original) {
		t.Fatal("input mutation changed dictionary")
	}
	must(t, b.AddBase("additional", nil))
	third, err := b.Build("packs/")
	must(t, err)
	if third.Key == second.Key || len(third.LinkKeys) != 3 {
		t.Fatal("Build did not allow further additions")
	}
	raw, err := packblob.EncodeRaw(original)
	must(t, err)
	clear(original)
	if bytes.Equal(readEnvelope(t, raw)[1].(fields)[1].(fields)[1].([]byte), original) {
		t.Fatal("EncodeRaw retained input buffer")
	}
}

func TestDictionaryMagic(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		want error
	}{
		{name: "short raw dictionary", size: 4},
		{name: "below format detection boundary", size: 7},
		{name: "format detection boundary", size: 8, want: packblob.ErrInvalidDictionary},
		{name: "formatted dictionary prefix", size: 24, want: packblob.ErrInvalidDictionary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := append([]byte{0x37, 0xa4, 0x30, 0xec}, bytes.Repeat([]byte("x"), tc.size-4)...)
			var b packblob.Builder
			must(t, b.AddBase("base", data)) // Always valid as an independent blob.
			before, err := b.Build("")
			must(t, err)
			if err := b.AddDelta("base", "child", []byte("child")); !errors.Is(err, tc.want) {
				t.Fatalf("AddDelta error = %v, want %v", err, tc.want)
			}
			if tc.want != nil {
				after, err := b.Build("")
				must(t, err)
				if !bytes.Equal(before.Data, after.Data) {
					t.Fatal("rejected dictionary changed builder")
				}
			}
		})
	}
}

func TestThriftRejectsInvalidUTF8Keys(t *testing.T) {
	invalidDictionary := "\xff"
	for _, tc := range []struct {
		name    string
		key     string
		entries []packblobthrift.Entry
	}{
		{name: "storage key", key: "\xff"},
		{name: "entry key", entries: []packblobthrift.Entry{{Key: "\xff"}}},
		{name: "dictionary key", entries: []packblobthrift.Entry{{Key: "valid", Dictionary: &invalidDictionary}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := packblobthrift.MarshalPack(tc.key, tc.entries); !errors.Is(err, packblobthrift.ErrInvalidString) {
				t.Fatalf("MarshalPack error = %v, want ErrInvalidString", err)
			}
		})
	}
}

// Decode envelopes with Apache Thrift or the generated protobuf schema.
type fields map[int16]any

func readEnvelope(t *testing.T, data []byte) fields {
	t.Helper()
	if len(data) < 4 {
		t.Fatal("missing envelope header")
	}
	switch binary.BigEndian.Uint32(data[:4]) {
	case 1:
		var envelope packblobpb.StorageEnvelope
		must(t, proto.Unmarshal(data[4:], &envelope))
		return protobufFields(t, envelope.ProtoReflect())
	case 0:
		// The legacy encoding remains independently decoded by Apache Thrift.
	default:
		t.Fatal("invalid envelope header")
	}
	transport := thrift.NewTMemoryBuffer()
	_, err := transport.Write(data[4:])
	must(t, err)
	result := readFields(t, thrift.NewTCompactProtocol(transport))
	if transport.Len() != 0 {
		t.Fatalf("trailing Thrift bytes: %d", transport.Len())
	}
	return result
}

// Traverse generated descriptors instead of duplicating each schema's fields.
func protobufFields(t *testing.T, message protoreflect.Message) fields {
	t.Helper()
	if len(message.GetUnknown()) != 0 {
		t.Fatal("unknown protobuf fields")
	}
	result := make(fields)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		id := int16(field.Number())
		if field.IsList() {
			list := value.List()
			entries := make([]fields, list.Len())
			for i := range entries {
				entries[i] = protobufFields(t, list.Get(i).Message())
			}
			result[id] = entries
		} else {
			switch field.Kind() {
			case protoreflect.MessageKind:
				result[id] = protobufFields(t, value.Message())
			case protoreflect.StringKind:
				result[id] = []byte(value.String())
			case protoreflect.BytesKind:
				result[id] = value.Bytes()
			default:
				t.Fatalf("unexpected protobuf kind %s", field.Kind())
			}
		}
		return true
	})
	return result
}

func readFields(t *testing.T, protocol thrift.TProtocol) fields {
	t.Helper()
	ctx := context.Background()
	_, err := protocol.ReadStructBegin(ctx)
	must(t, err)
	result := make(fields)
	for {
		_, typ, id, err := protocol.ReadFieldBegin(ctx)
		must(t, err)
		if typ == thrift.STOP {
			break
		}
		if _, exists := result[id]; exists {
			t.Fatalf("duplicate field %d", id)
		}
		switch typ {
		case thrift.STRING:
			result[id], err = protocol.ReadBinary(ctx)
			must(t, err)
		case thrift.STRUCT:
			result[id] = readFields(t, protocol)
		case thrift.LIST:
			elem, size, err := protocol.ReadListBegin(ctx)
			must(t, err)
			if elem != thrift.STRUCT || size < 0 {
				t.Fatalf("unexpected list type/size: %v/%d", elem, size)
			}
			list := make([]fields, size)
			for i := range list {
				list[i] = readFields(t, protocol)
			}
			must(t, protocol.ReadListEnd(ctx))
			result[id] = list
		default:
			t.Fatalf("unexpected Thrift type %v", typ)
		}
		must(t, protocol.ReadFieldEnd(ctx))
	}
	must(t, protocol.ReadStructEnd(ctx))
	return result
}

func decompress(t *testing.T, compressed, dictionary []byte, useDictionary bool) []byte {
	t.Helper()
	opts := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
	if useDictionary {
		opts = append(opts, zstd.WithDecoderDictRaw(0, dictionary))
	}
	decoder, err := zstd.NewReader(nil, opts...)
	must(t, err)
	defer decoder.Close()
	data, err := decoder.DecodeAll(compressed, nil)
	must(t, err)
	return data
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestEncodingOptions(t *testing.T) {
	raw, err := packblob.EncodeRaw(nil)
	must(t, err)
	if binary.BigEndian.Uint32(raw[:4]) != 1 {
		t.Fatal("EncodeRaw must default to protobuf")
	}
	single := readEnvelope(t, raw)[1].(fields)[1].(fields)
	if _, selected := single[1]; len(single) != 1 || !selected {
		t.Fatal("empty raw oneof was not selected")
	}
	invalid := packblob.WithEncoding(packblob.Encoding(255))
	if _, err := packblob.NewBuilder(0, invalid); !errors.Is(err, packblob.ErrInvalidEncoding) {
		t.Fatalf("NewBuilder error = %v, want ErrInvalidEncoding", err)
	}
	if _, err := packblob.EncodeRaw(nil, invalid); !errors.Is(err, packblob.ErrInvalidEncoding) {
		t.Fatalf("EncodeRaw error = %v, want ErrInvalidEncoding", err)
	}
	for _, data := range [][]byte{nil, []byte("tiny"), bytes.Repeat([]byte("compressible"), 100)} {
		if _, err := packblob.EncodeSingle(data, 0, invalid); !errors.Is(err, packblob.ErrInvalidEncoding) {
			t.Fatalf("EncodeSingle error = %v, want ErrInvalidEncoding", err)
		}
	}
}

func TestEncodingIdentityAndEmptyKeys(t *testing.T) {
	var protobufBuilder packblob.Builder // The zero value selects protobuf.
	thriftBuilder, err := packblob.NewBuilder(0, packblob.WithEncoding(packblob.Thrift))
	must(t, err)
	var identities []string
	for _, tc := range []struct {
		name     string
		builder  *packblob.Builder
		encoding packblob.Encoding
		header   uint32
		suffix   string
	}{
		{"zero builder protobuf", &protobufBuilder, packblob.Protobuf, 1, ".pack.pb"},
		{"explicit thrift", thriftBuilder, packblob.Thrift, 0, ".pack"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			must(t, tc.builder.AddBase("", nil))
			must(t, tc.builder.AddDelta("", "child", []byte("child value")))
			blob, err := tc.builder.Build("packs/")
			must(t, err)
			if blob.Encoding != tc.encoding || binary.BigEndian.Uint32(blob.Data[:4]) != tc.header {
				t.Fatalf("encoding metadata/header mismatch: %d, %x", blob.Encoding, blob.Data[:4])
			}
			if !strings.HasSuffix(blob.Key, tc.suffix) {
				t.Fatalf("key %q lacks suffix %q", blob.Key, tc.suffix)
			}
			identities = append(identities, strings.TrimSuffix(blob.Key, tc.suffix))
			if !slices.Equal(blob.LinkKeys, []string{"", "child"}) {
				t.Fatalf("link keys = %q", blob.LinkKeys)
			}
			pack := readEnvelope(t, blob.Data)[1].(fields)[2].(fields)
			entries := pack[2].([]fields)
			if len(entries) != 2 || string(entries[0][1].([]byte)) != "" || string(entries[1][1].([]byte)) != "child" {
				t.Fatal("empty key or insertion order lost")
			}
			base := decompress(t, entries[0][2].(fields)[1].(fields)[2].([]byte), nil, false)
			delta := entries[1][2].(fields)[2].(fields)
			if len(base) != 0 || string(delta[1].([]byte)) != "" {
				t.Fatal("empty base contents or dictionary key changed")
			}
			if actual := decompress(t, delta[2].([]byte), base, true); string(actual) != "child value" {
				t.Fatalf("delta contents = %q", actual)
			}
		})
	}
	if len(identities) != 2 || identities[0] != identities[1] {
		t.Fatalf("encoding changed logical-key digest: %q", identities)
	}
	t.Run("logical aliases stay disjoint across encodings", func(t *testing.T) {
		// These logical keys collided when protobuf used the .pb.pack suffix.
		// Use returned link keys to model the underlying blobstore aliases.
		var aliases []string
		for _, tc := range []struct {
			encoding packblob.Encoding
			key      string
			suffix   string
		}{
			{packblob.Protobuf, "content.base", packblob.EnvelopeSuffix},
			{packblob.Thrift, "content.base.pb", packblob.ThriftEnvelopeSuffix},
		} {
			builder, err := packblob.NewBuilder(0, packblob.WithEncoding(tc.encoding))
			must(t, err)
			must(t, builder.AddBase(tc.key, []byte("contents")))
			blob, err := builder.Build("")
			must(t, err)
			if len(blob.LinkKeys) != 1 || blob.LinkKeys[0] != tc.key {
				t.Fatalf("unexpected logical link keys: %q", blob.LinkKeys)
			}
			aliases = append(aliases, blob.LinkKeys[0]+tc.suffix)
		}
		if aliases[0] == aliases[1] {
			t.Fatalf("protobuf and Thrift logical aliases collide at %q", aliases[0])
		}
	})
}
