// Package packblob creates dictionary-compressed indexed files, storage
// envelopes, and standalone delta packets.
//
// NewIndexedWriter streams multiple versions into one file. Each delta refers
// to an independent anchor in that file; delta chains are not allowed. The
// writer retains metadata, not raw base bytes, and Close writes the final index.
// OpenIndexed reads only the header and footer from an io.ReaderAt. ReadVersion
// searches the on-disk index and loads only the requested payload and, for a
// delta, its anchor. Caller-supplied limits bound both payloads and decompression;
// SHA-256 hashes and sizes verify the result. Callers manage version labels,
// anchor selection, file lifetime, and atomic publication. Indexed files use a
// distinct format, not a protobuf or Thrift storage envelope.
// Lookup and EntryAt expose metadata for an application's digest-to-pack catalog.
// ReadDelta reads only a delta frame, so a server can send it without reading the
// anchor when the client already has that base. DecodeDelta must verify the
// reconstructed target at the recipient; ReadDelta alone cannot verify it.
//
// A Builder compresses independent base blobs with Zstandard and other blobs
// using an earlier blob's uncompressed contents as a raw Zstandard dictionary.
// Build returns the serialized envelope, its storage key, and sorted logical
// keys for the caller to link in an underlying blobstore. This package does not
// write to a blobstore or read these protobuf/Thrift pack envelopes.
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
