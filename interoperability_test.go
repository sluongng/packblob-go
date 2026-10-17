package packblob

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
)

// TestInteroperability checks the wire format using implementations independent
// of this package: Apache Thrift for Compact Protocol and libzstd for decoding.
// Set PACKBLOB_INTEROP_PYTHON to a Python interpreter with thrift installed.
func TestInteroperability(t *testing.T) {
	python := os.Getenv("PACKBLOB_INTEROP_PYTHON")
	if python == "" {
		t.Skip("set PACKBLOB_INTEROP_PYTHON to enable Apache Thrift/libzstd validation")
	}
	type entry struct {
		Key                string `json:"key"`
		Data               []byte `json:"data"`
		Variant            string `json:"variant"`
		DictKey            string `json:"dict_key"`
		RequiresDictionary bool   `json:"requires_dictionary"`
	}
	type envelope struct {
		Name     string   `json:"name"`
		Kind     string   `json:"kind"`
		Data     []byte   `json:"data"`
		Prefix   string   `json:"prefix"`
		Key      string   `json:"key"`
		LinkKeys []string `json:"link_keys"`
		Entries  []entry  `json:"entries"`
	}
	var cases []envelope
	for _, tc := range []struct {
		name    string
		data    []byte
		raw     bool
		variant string
	}{
		{name: "explicit raw", data: []byte{0, 1, 2, 255}, raw: true, variant: "raw"},
		{name: "empty raw", data: []byte{}, raw: true, variant: "raw"},
		{name: "small single stays raw", data: []byte("small"), variant: "raw"},
		{name: "empty single stays raw", data: []byte{}, variant: "raw"},
		{name: "compressible single", data: bytes.Repeat([]byte("mononoke packed blob\n"), 2048), variant: "zstd"},
	} {
		var data []byte
		var err error
		if tc.raw {
			data, err = EncodeRaw(tc.data)
		} else {
			data, err = EncodeSingle(tc.data, 3)
		}
		if err != nil {
			t.Fatalf("%s: encode: %v", tc.name, err)
		}
		cases = append(cases, envelope{Name: tc.name, Kind: "single", Data: data,
			Entries: []entry{{Data: tc.data, Variant: tc.variant}}})
	}

	builder, err := NewBuilder(3)
	if err != nil {
		t.Fatal(err)
	}
	base := bytes.Repeat([]byte("The dictionary contains shared content for related revisions.\n"), 1024)
	chain := append(bytes.Clone(base), []byte("first revision\n")...)
	entries := []entry{
		{Key: "z-base", Data: base, Variant: "zstd"},
		{Key: "a-child", Data: chain, Variant: "delta", DictKey: "z-base"},
		{Key: "é-chain", Data: append(bytes.Clone(chain), []byte("second revision\n")...), Variant: "delta", DictKey: "a-child"},
		{Key: "shared", Data: append(bytes.Clone(base), []byte("independent revision\n")...), Variant: "delta", DictKey: "z-base"},
		{Key: "empty-delta", Data: []byte{}, Variant: "delta", DictKey: "z-base"},
		{Key: "empty-base", Data: []byte{}, Variant: "zstd"},
		{Key: "empty-dictionary-child", Data: []byte("value with empty dictionary"), Variant: "delta", DictKey: "empty-base"},
		{Key: "short-base", Data: []byte("short"), Variant: "zstd"},
		{Key: "short-dictionary-child", Data: []byte("short content using a short dictionary"), Variant: "delta", DictKey: "short-base"},
		{Key: "", Data: []byte("empty key dictionary"), Variant: "zstd"},
		{Key: "empty-key-child", Data: []byte("empty key dictionary with a child"), Variant: "delta", DictKey: ""},
	}
	// Compact Protocol uses a distinct extended list header at 15 entries.
	for i := 0; i < 12; i++ {
		entries = append(entries, entry{Key: fmt.Sprintf("revision-%02d", i),
			Data:    append(bytes.Clone(base), []byte(fmt.Sprintf("revision %d", i))...),
			Variant: "delta", DictKey: "z-base"})
	}
	for _, e := range entries {
		if e.Variant == "delta" {
			err = builder.AddDelta(e.DictKey, e.Key, e.Data)
		} else {
			err = builder.AddBase(e.Key, e.Data)
		}
		if err != nil {
			t.Fatalf("add %q: %v", e.Key, err)
		}
	}
	const prefix = "test/packed/"
	blob, err := builder.Build(prefix)
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, envelope{Name: "chained and shared dictionaries with extended list",
		Kind: "pack", Data: blob.Data, Prefix: prefix, Key: blob.Key, LinkKeys: blob.LinkKeys, Entries: entries})

	// High-entropy contents cannot be compressed as repeated literals. The
	// independent decoder also checks that the delta cannot be decoded to the
	// expected contents without its raw dictionary.
	random := rand.New(rand.NewSource(42))
	entropyBase := make([]byte, 64<<10)
	if _, err := random.Read(entropyBase); err != nil {
		t.Fatal(err)
	}
	entropyDelta := bytes.Clone(entropyBase)
	if _, err := random.Read(entropyDelta[len(entropyDelta)-(1<<10):]); err != nil {
		t.Fatal(err)
	}
	for _, level := range []int{-1, 0, 3, 7, 19, 22} {
		name := fmt.Sprintf("high-entropy raw dictionary level %d", level)
		builder, err := NewBuilder(level)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := builder.AddBase("base", entropyBase); err != nil {
			t.Fatalf("%s: add base: %v", name, err)
		}
		if err := builder.AddDelta("base", "delta", entropyDelta); err != nil {
			t.Fatalf("%s: add delta: %v", name, err)
		}
		blob, err := builder.Build(prefix)
		if err != nil {
			t.Fatalf("%s: build: %v", name, err)
		}
		cases = append(cases, envelope{Name: name, Kind: "pack", Data: blob.Data,
			Prefix: prefix, Key: blob.Key, LinkKeys: blob.LinkKeys, Entries: []entry{
				{Key: "base", Data: entropyBase, Variant: "zstd"},
				{Key: "delta", Data: entropyDelta, Variant: "delta", DictKey: "base", RequiresDictionary: true},
			}})
	}

	// libzstd detects a formatted dictionary only with at least eight bytes.
	// Shorter magic-prefixed contents remain valid raw dictionaries; longer
	// values can be stored independently but cannot safely serve as dictionaries.
	for _, size := range []int{7, 8, 24} {
		name := fmt.Sprintf("dictionary format magic with %d bytes", size)
		dictionary := make([]byte, size)
		copy(dictionary, []byte{0x37, 0xa4, 0x30, 0xec})
		builder, err := NewBuilder(3)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := builder.AddBase("base", dictionary); err != nil {
			t.Fatalf("%s: add base: %v", name, err)
		}
		entries := []entry{{Key: "base", Data: dictionary, Variant: "zstd"}}
		data := bytes.Repeat(dictionary, 64)
		err = builder.AddDelta("base", "delta", data)
		if size < 8 {
			if err != nil {
				t.Fatalf("%s: add raw-dictionary delta: %v", name, err)
			}
			entries = append(entries, entry{Key: "delta", Data: data, Variant: "delta", DictKey: "base"})
		} else if !errors.Is(err, ErrInvalidDictionary) {
			t.Fatalf("%s: add delta error = %v, want ErrInvalidDictionary", name, err)
		}
		blob, err := builder.Build(prefix)
		if err != nil {
			t.Fatalf("%s: build: %v", name, err)
		}
		// The independent decoder verifies rejected additions did not appear in
		// either the serialized entries, the pack-key hash, or the link keys.
		cases = append(cases, envelope{Name: name, Kind: "pack", Data: blob.Data,
			Prefix: prefix, Key: blob.Key, LinkKeys: blob.LinkKeys, Entries: entries})
	}

	manifest, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "testdata/interop.py")
	command.Stdin = bytes.NewReader(manifest)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("independent decoder: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}
