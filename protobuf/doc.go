// Package protobuf defines a Protocol Buffers schema for the packblob data
// model. It mirrors the model in Mononoke's packblob Thrift schema, including
// ordered entries and explicit union variants, but not its wire encoding.
// It uses Edition 2024 with explicit scalar presence and Go's Opaque API.
// Construct messages with setters or generated builders, and use Has methods
// to distinguish missing fields from explicitly supplied empty values.
//
// These messages are not compatible with Mononoke's header-0 Thrift compact
// envelopes. This package does not define a protobuf envelope header or change
// the wire format emitted by the root packblob package. Applications that use
// these messages must define their own transport or storage framing.
package protobuf

//go:generate sh generate.sh
