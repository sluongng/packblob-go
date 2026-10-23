package protobuf

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestSingleValuePreservesEmptyRaw(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    *SingleValue
		wantRaw  bool
		wantWire []byte
	}{
		{name: "unset", value: &SingleValue{}},
		{name: "explicit empty raw", value: SingleValue_builder{Raw: []byte{}}.Build(), wantRaw: true, wantWire: []byte{0x0a, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := proto.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			// Preserve the proto3 encoding of the existing oneof alternatives.
			if !bytes.Equal(encoded, tc.wantWire) {
				t.Fatalf("wire bytes = %x, want %x", encoded, tc.wantWire)
			}
			var decoded SingleValue
			if err := proto.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			gotRaw := decoded.HasRaw()
			if gotRaw != tc.wantRaw {
				t.Fatalf("raw variant selected = %t, want %t", gotRaw, tc.wantRaw)
			}
			if !proto.Equal(tc.value, &decoded) {
				t.Fatalf("round trip changed value: got %v, want %v", &decoded, tc.value)
			}
		})
	}
}

func TestEnvelopePreservesOrderedDictionaryReferences(t *testing.T) {
	base := SingleValue_builder{Raw: []byte("dictionary")}.Build()
	delta := ZstdFromDictValue_builder{DictKey: new("base"), Zstd: []byte("compressed delta")}.Build()
	chained := ZstdFromDictValue_builder{DictKey: new("delta"), Zstd: []byte("compressed chained delta")}.Build()
	pack := PackedFormat_builder{
		Key: new("pack-key"),
		Entries: []*PackedEntry{
			PackedEntry_builder{Key: new("base"), Data: PackedValue_builder{Single: base}.Build()}.Build(),
			PackedEntry_builder{Key: new("delta"), Data: PackedValue_builder{ZstdFromDict: delta}.Build()}.Build(),
			PackedEntry_builder{Key: new("chained"), Data: PackedValue_builder{ZstdFromDict: chained}.Build()}.Build(),
		},
	}.Build()
	want := StorageEnvelope_builder{Storage: StorageFormat_builder{Packed: pack}.Build()}.Build()
	encoded, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got StorageEnvelope
	if err := proto.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &got) {
		t.Fatalf("round trip changed pack or entry order: got %v, want %v", &got, want)
	}
}

func TestEnvelopePreservesExplicitEmptyKeys(t *testing.T) {
	for _, tc := range []struct {
		name        string
		key         *string
		wantPresent bool
	}{
		{name: "missing keys"},
		{name: "explicit empty keys", key: new(""), wantPresent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dictionary := ZstdFromDictValue_builder{DictKey: tc.key}.Build()
			entry := PackedEntry_builder{Key: tc.key, Data: PackedValue_builder{ZstdFromDict: dictionary}.Build()}.Build()
			encoded, err := proto.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			var decoded PackedEntry
			if err := proto.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.HasKey() != tc.wantPresent {
				t.Fatalf("entry key presence = %t, want %t", decoded.HasKey(), tc.wantPresent)
			}
			gotDictionary := decoded.GetData().GetZstdFromDict()
			if gotDictionary.HasDictKey() != tc.wantPresent {
				t.Fatalf("dictionary key presence = %t, want %t", gotDictionary.HasDictKey(), tc.wantPresent)
			}
			if decoded.GetKey() != "" || gotDictionary.GetDictKey() != "" {
				t.Fatal("empty key values changed during round trip")
			}
		})
	}
}
