// Package thrift encodes Mononoke packblob storage envelopes using Thrift
// Compact Protocol. The schema is defined in packblob.thrift. These functions
// serialize already-compressed values; they do not compress data or validate
// dictionary dependencies and pack identities. Use the root packblob package
// to build packs from raw data.
package thrift

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"
)

var (
	ErrTooLarge      = errors.New("packblob/thrift: value exceeds Thrift size limit")
	ErrInvalidString = errors.New("packblob/thrift: keys must be valid UTF-8")
)

// Entry is an already-compressed packed entry. A nil Dictionary selects an
// independent SingleValue.Zstd; otherwise it selects ZstdFromDict and names an
// earlier dictionary entry. A pointer to an empty string names the empty key.
type Entry struct {
	Key        string
	Zstd       []byte
	Dictionary *string
}

// This encoder is limited to the packblob schema's structs, unions, byte
// strings, and lists. There is no RPC message header. All fields are emitted
// in ascending ID order, so their compact headers use a field-ID delta.
const (
	compactStop   = 0
	compactBinary = 8
	compactList   = 9
	compactStruct = 12
)

type compactWriter []byte

func (w *compactWriter) field(delta, typ byte) {
	*w = append(*w, delta<<4|typ)
}

func (w *compactWriter) stop() {
	*w = append(*w, compactStop)
}

func (w *compactWriter) binary(data []byte) {
	*w = binary.AppendUvarint(*w, uint64(len(data)))
	*w = append(*w, data...)
}

func (w *compactWriter) list(size int) {
	if size < 15 {
		*w = append(*w, byte(size)<<4|compactStruct)
	} else {
		*w = append(*w, 0xf0|compactStruct)
		*w = binary.AppendUvarint(*w, uint64(size))
	}
}

func (w *compactWriter) single(data []byte, compressed bool) {
	id := byte(1) // SingleValue.Raw
	if compressed {
		id = 2 // SingleValue.Zstd
	}
	w.field(id, compactBinary)
	w.binary(data)
	w.stop() // SingleValue union
}

// MarshalSingle serializes an independent payload as a complete Mononoke
// envelope, including the four-byte zero header. When compressed is true,
// data must already be a Zstandard frame. Otherwise data is stored as Raw.
// The returned slice does not alias data.
func MarshalSingle(data []byte, compressed bool) ([]byte, error) {
	if len(data) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	w := compactWriter{0, 0, 0, 0}
	w.field(1, compactStruct) // StorageEnvelope.storage
	w.field(1, compactStruct) // StorageFormat.Single
	w.single(data, compressed)
	w.stop() // StorageFormat
	w.stop() // StorageEnvelope
	return w, nil
}

// MarshalPack serializes already-compressed entries as a complete Mononoke
// envelope with the given embedded storage key. It preserves entry order and
// does not take ownership of or retain the input slices.
func MarshalPack(key string, entries []Entry) ([]byte, error) {
	if len(key) > math.MaxInt32 || len(entries) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	if !utf8.ValidString(key) {
		return nil, ErrInvalidString
	}
	for _, e := range entries {
		if len(e.Key) > math.MaxInt32 || len(e.Zstd) > math.MaxInt32 || (e.Dictionary != nil && len(*e.Dictionary) > math.MaxInt32) {
			return nil, ErrTooLarge
		}
		if !utf8.ValidString(e.Key) || (e.Dictionary != nil && !utf8.ValidString(*e.Dictionary)) {
			return nil, ErrInvalidString
		}
	}
	w := compactWriter{0, 0, 0, 0}
	w.field(1, compactStruct) // StorageEnvelope.storage
	w.field(2, compactStruct) // StorageFormat.Packed
	w.field(1, compactBinary) // PackedFormat.key
	w.binary([]byte(key))
	w.field(1, compactList) // PackedFormat.entries (field ID 2)
	w.list(len(entries))
	for _, e := range entries {
		w.field(1, compactBinary) // PackedEntry.key
		w.binary([]byte(e.Key))
		w.field(1, compactStruct) // PackedEntry.data
		if e.Dictionary != nil {
			w.field(2, compactStruct) // PackedValue.ZstdFromDict
			w.field(1, compactBinary) // ZstdFromDictValue.dict_key
			w.binary([]byte(*e.Dictionary))
			w.field(1, compactBinary) // ZstdFromDictValue.zstd
			w.binary(e.Zstd)
			w.stop() // ZstdFromDictValue
		} else {
			w.field(1, compactStruct) // PackedValue.Single
			w.single(e.Zstd, true)
		}
		w.stop() // PackedValue
		w.stop() // PackedEntry
	}
	w.stop() // PackedFormat
	w.stop() // StorageFormat
	w.stop() // StorageEnvelope
	return w, nil
}
