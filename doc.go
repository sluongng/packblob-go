// Package packblob creates Mononoke packblob storage envelopes.
//
// A Builder compresses independent base blobs with Zstandard and other blobs
// using an earlier blob's uncompressed contents as a raw Zstandard dictionary.
// Build returns the serialized envelope, its storage key, and sorted logical
// keys for the caller to link in an underlying blobstore. This package does not
// write to a blobstore or implement a pack reader.
//
// Envelopes use a four-byte zero header followed by Thrift Compact Protocol.
// Pack identities match 64-bit Mononoke: keyed BLAKE2b-256 over sorted keys,
// with each UTF-8 byte length encoded as a little-endian uint64. The identity
// depends on keys, not their values; callers must use immutable blob keys.
//
// The thrift subpackage contains the schema and low-level envelope encoder.
// The protobuf subpackage provides the same data model as Protocol Buffers
// messages; those messages are not Mononoke-compatible storage envelopes.
package packblob
