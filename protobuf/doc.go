// Package protobuf defines a Protocol Buffers schema for the packblob data
// model. It mirrors the model in Mononoke's packblob Thrift schema, including
// ordered entries and explicit union variants, but not its wire encoding.
// It uses Edition 2024 with explicit scalar presence and Go's Opaque API.
// Construct messages with setters or generated builders, and use Has methods
// to distinguish missing fields from explicitly supplied empty values.
//
// These messages are not compatible with Mononoke's header-0 Thrift compact
// envelopes. The root packblob package frames protobuf messages with a separate
// four-byte big-endian header of 1. Direct proto.Marshal calls on these messages
// produce only the protobuf body, without that storage header.
package protobuf

//go:generate sh generate.sh
