// Package protobuf defines a Protocol Buffers schema for the packblob data
// model. It mirrors the model in Mononoke's packblob Thrift schema, including
// ordered entries and explicit union variants, but not its wire encoding.
// It uses Edition 2024 with explicit scalar presence and Go's Opaque API.
// Construct messages with setters or generated builders, and use Has methods
// to distinguish missing fields from explicitly supplied empty values.
//
// These messages are not compatible with Mononoke's header-0 Thrift compact
// envelopes. The root packblob package frames StorageEnvelope messages with a
// separate four-byte big-endian header of 1. Direct proto.Marshal calls produce
// only the protobuf body, without that storage header.
//
// StandaloneDelta and BlobDigest extend the model for external-base transfers.
// A StandaloneDelta contains base and target identities and compressed target
// bytes, but no base contents. It is a bare protobuf message, not a
// StorageEnvelope, and has no four-byte storage header.
package protobuf

//go:generate sh generate.sh
