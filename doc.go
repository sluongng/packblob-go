// Package packblob creates dictionary-compressed blob packs with a choice of
// protobuf or Mononoke-compatible Thrift storage envelopes.
//
// A Builder compresses independent base blobs with Zstandard and other blobs
// using an earlier blob's uncompressed contents as a raw Zstandard dictionary.
// Build returns the serialized envelope, its storage key, and sorted logical
// keys for the caller to link in an underlying blobstore. This package does not
// write to a blobstore or implement a pack reader.
//
// EncodeDelta and DecodeDelta provide a separate standalone codec for sending
// only a delta when both hosts already have a negotiated base. Its bare protobuf
// packet contains SHA-256 base and target identities and a raw-dictionary Zstd
// frame, never the base contents. DecodeDelta requires an independently known
// target digest and an output limit, and verifies both base and target contents.
// Base negotiation, retention, transport, and CAS integration are caller concerns.
//
// Protobuf is the default, including for a zero-value Builder. Use
// WithEncoding(Thrift) with NewBuilder, EncodeSingle, or EncodeRaw when writing
// for Mononoke's existing reader. Both encodings have a four-byte big-endian
// header: 1 for protobuf (a library extension), or 0 for Thrift Compact Protocol.
// Protobuf uses the .pack.pb storage suffix; Thrift retains Mononoke's .pack.
// Pack identities match 64-bit Mononoke: keyed BLAKE2b-256 over sorted keys,
// with each UTF-8 byte length encoded as a little-endian uint64. The identity
// digest depends on keys, not their values; callers must use immutable blob
// keys. Encoding-specific suffixes separate the resulting storage keys.
//
// The thrift subpackage contains the schema and low-level envelope encoder.
// The protobuf subpackage provides the same data model as Protocol Buffers
// messages; those messages are not Mononoke-compatible storage envelopes.
package packblob
