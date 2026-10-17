package packblob_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/klauspost/compress/zstd"
	"github.com/sluongng/packblob-go"
	packblobthrift "github.com/sluongng/packblob-go/thrift"
)

func TestIndependentEnvelopes(t *testing.T) {
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
			encoded, err := packblob.EncodeSingle(tc.input, 0)
			must(t, err)
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
	// A hand-auditable golden tests framing and the Raw union independently
	// of the test reader and compression implementation.
	encoded, err := packblob.EncodeRaw([]byte("hello"))
	must(t, err)
	if got, want := hex.EncodeToString(encoded), "000000001c1c180568656c6c6f000000"; got != want {
		t.Fatalf("raw envelope = %s, want %s", got, want)
	}
}

func TestPack(t *testing.T) {
	var builder packblob.Builder // zero value uses default compression
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
	const wantKey = "49df56ffd39791720f4d4b5c05d3156dfeab994fcb6b0000b3c2343280b95975.pack"
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

// Decode the wire using Apache Thrift, not the package's schema encoder.
type fields map[int16]any

func readEnvelope(t *testing.T, data []byte) fields {
	t.Helper()
	if len(data) < 4 || !bytes.Equal(data[:4], []byte{0, 0, 0, 0}) {
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
