# packblob-go

`packblob` is a pure-Go library for dictionary-compressed blob packs and standalone delta transfers, with protobuf and Mononoke-compatible Thrift pack encodings.
It combines related blobs into an ordered pack, using earlier blobs as Zstandard dictionaries.
It returns the encoded bytes and storage keys without accessing a blobstore.
**Protobuf is the default.** Select Thrift explicitly when writing for Mononoke's existing reader.

Requires Go 1.26 or later.
The library does not require CGo or a system Zstandard installation.

## Create a pack

```go
package example

import "github.com/sluongng/packblob-go"

func PackRevisions(base, next []byte) (*packblob.Blob, error) {
    builder, err := packblob.NewBuilder(0) // Default compression.
    if err != nil {
        return nil, err
    }
    if err := builder.AddBase("content.base", base); err != nil {
        return nil, err
    }
    if err := builder.AddDelta("content.base", "content.next", next); err != nil {
        return nil, err
    }
    return builder.Build("packed/")
}
```

The zero value of `Builder` also uses protobuf and default compression.
`AddBase` always emits a standalone Zstandard frame, even for empty data.
`AddDelta` always emits a dictionary-compressed entry and requires its dictionary key to exist already.
A delta can itself serve as a dictionary for a later entry.
Duplicate keys and forward references are rejected without changing the builder.

`Build` returns:

- `Encoding`: the selected wire encoding, `Protobuf` or `Thrift`.
- `Key`: the pack's storage identity, including your prefix and the encoding-specific suffix.
- `LinkKeys`: sorted logical entry keys, without repository prefixes or storage suffixes.
- `Data`: the complete storage envelope, including the four-byte format header.

Write `Data` under `Key`, then create a hardlink-like copy for each `<repository-prefix><logical-key><suffix>`, using the suffix for the selected encoding.
With Thrift selected, this matches Mononoke's underlying storage layout; Mononoke's own writer removes the temporary pack key after those copies succeed.
Storage writes, copy semantics, error recovery, and removal are the caller's responsibility.
The pack prefix passed to `Build` is independent of the repository prefix applied to links.
Pass unprefixed keys to `AddBase` and `AddDelta`; recognized `repoNNNN.` and `ephN.repoNNNN.` prefixes are rejected.

## Independent blobs

Use `EncodeSingle(data, level)` to create a standalone envelope.
It stores the payload as Zstandard only when compression makes it strictly smaller; otherwise it stores raw bytes.
Use `EncodeRaw(data)` to force raw storage.
Both functions default to protobuf and accept the same `WithEncoding` option as `NewBuilder`.
Store either result at the logical blob key plus the selected storage suffix.

## Transfer only a delta

Use `EncodeDelta` when sender and receiver have already negotiated a shared base.
Unlike `Builder.AddDelta`, this API does **not** include the base in its output.
The packet contains SHA-256 base and target digests (hash and uncompressed size), an explicit digest-function identifier, and a raw-dictionary Zstandard frame.

```go
// Sender: base is already available at the receiver.
delta, err := packblob.EncodeDelta(base, target, 0)
if err != nil {
    return err
}
wire, err := delta.MarshalBinary()
if err != nil {
    return err
}
// Send wire, not base.

// Receiver: expectedTarget comes independently from the upload/download request,
// not from the received packet. Choose limits appropriate to your service.
received, err := packblob.UnmarshalDelta(wire, 2<<20)
if err != nil {
    return err
}
target, err = packblob.DecodeDelta(base, received, expectedTarget, 4<<20)
if err != nil {
    return err
}
```

`DecodeDelta` verifies the local base, checks the packet against the independently expected target, bounds decompression, and verifies the reconstructed target's hash and size.
Every error returns nil output.
Empty bases and targets are supported, as are arbitrary raw base bytes, including dictionary-magic prefixes.
The initial codec supports **SHA-256 only**; its digests are independent of pack storage keys.
Packets use the bare protobuf `StandaloneDelta` message, without a storage-envelope header or suffix; `WithEncoding` applies to packs and independent envelopes, not this standalone codec.

This is a building block for a future Bazel Remote Execution API adapter, **not an implemented REAPI extension or Bazel integration**.
Applications must provide base negotiation, transport, and fallback policy.
Compare the complete serialized packet against an ordinary raw or Zstandard target transfer before choosing a delta: metadata overhead can outweigh savings for small blobs.
The one-shot API creates fresh codec contexts; measure CPU and allocations as well as transferred bytes before selecting production thresholds.
If the base is missing or a delta is not worthwhile, fall back to transferring the full **target**, not uploading the base first.
Malformed frames or digest mismatches must be rejected.

For CAS uploads, reconstruct and verify the target before publishing it under its canonical digest.
The target must remain readable through the ordinary CAS interface.
You can also store a delta separately from its base, but storage that retains delta dependencies must keep the base available and reconstruct transparently on reads.
This library does not pin bases, track dependency lifetimes, or implement a blobstore.

## Choose an encoding

```go
builder, err := packblob.NewBuilder(0, packblob.WithEncoding(packblob.Thrift))
```

Handle `err`, then add entries and build as usual.
Pass the same option to `EncodeRaw(data, packblob.WithEncoding(packblob.Thrift))` or `EncodeSingle(data, level, packblob.WithEncoding(packblob.Thrift))` for independent Thrift envelopes.
Omit the option, or pass `WithEncoding(packblob.Protobuf)`, for protobuf.

| Encoding | Four-byte big-endian header | Storage suffix | Reader |
| --- | --- | --- | --- |
| Protobuf (default) | `1` | `.pack.pb` (`EnvelopeSuffix`) | Protobuf `StorageEnvelope` after the header |
| Thrift | `0` | `.pack` (`ThriftEnvelopeSuffix`) | Mononoke's existing packblob reader |

The protobuf header is a library extension, not an upstream Mononoke format.
For example, after checking the header, a protobuf reader can pass `blob.Data[4:]` to `proto.Unmarshal` with `protobuf.StorageEnvelope`.
The suffix applies to both the pack's storage key and its logical links, keeping protobuf and Thrift values in separate namespaces.

**Migration from the original Thrift-only API:** calls without an option now emit protobuf, and `EnvelopeSuffix` now means `.pack.pb`.
Add `WithEncoding(packblob.Thrift)` and use `ThriftEnvelopeSuffix` for existing Mononoke storage.
Explicit Thrift output retains its original header, key digest, and `.pack` suffix.

## Compatibility and limits

The Thrift encoding follows the upstream [schema](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/mononoke/blobstore/packblob/if/packblob.thrift), [envelope framing](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/mononoke/blobstore/packblob/src/envelope.rs), and [pack writer](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/mononoke/blobstore/packblob/src/pack.rs).

- Envelopes contain a four-byte encoding header followed by the selected encoding's `StorageEnvelope`.
- Pack identities use keyed BLAKE2b-256 with key `pack`, hashing sorted logical keys as an eight-byte little-endian UTF-8 byte length followed by the key bytes.
  This matches 64-bit Mononoke, whose Rust implementation encodes a native `usize` length.
- **Keys must identify immutable contents.** Pack identity depends only on the logical keys, not the payloads, entry order, or compression settings.
  Reusing a key for different contents can collide with an existing pack identity within the same encoding.
- Compression level `0` means the default, `3`.
  Other levels from `-131072` through `22` map to the pure-Go encoder's nearest preset.
  The output is decodable by libzstd but is not expected to match libzstd byte for byte or achieve the same compression ratio.
- Each builder retains raw dictionary bytes and compressed payloads for all entries.
  `Build` allocates another buffer for the envelope.
  Bound pack sizes in the caller; this is not a streaming API.
- Builders copy input data, and each `Build` returns independent slices.
  A builder can be extended after building but must not be copied after use or used concurrently.
- In a pack, a blob of at least eight bytes starting with Zstandard dictionary magic (`37 a4 30 ec`) cannot be used as a raw dictionary.
  `AddDelta` returns `ErrInvalidDictionary` because Mononoke's decoder would interpret those bytes as a formatted dictionary.
  The blob can still be stored as a base or standalone value.
- Keys and prefixes must be valid UTF-8.
  Both encodings limit individual strings, payloads, and entry counts to signed 32-bit lengths.
  The complete protobuf body must also fit within that limit.
- Standalone delta parsing and decoding require caller-supplied encoded and output limits, each at most signed 32-bit size.
  Decoding additionally limits the Zstandard window to 8 MiB.
- This package does not implement a pack reader, automatic dictionary selection, base negotiation, blobstore access, or a command-line tool.

## Schema packages

The module is `github.com/sluongng/packblob-go`; its root Go package is `packblob`.

- [`thrift`](thrift) contains the Mononoke-compatible [schema](thrift/packblob.thrift) and Compact Protocol encoder.
  Its `MarshalSingle` and `MarshalPack` functions wrap already-compressed values in the four-byte-header storage envelope.
  Use the root package for compression, dictionary ordering, and pack-key creation.
- [`protobuf`](protobuf) contains an equivalent [Protocol Buffers schema](protobuf/packblob.proto) and generated Go messages.
  Its `oneof` fields represent the Thrift unions, and its repeated entries preserve dictionary order.
  Edition 2024 provides explicit scalar presence and Go's Opaque API: use setters or generated builders to construct messages, and `Has` methods to distinguish missing keys from explicit empty keys.
  Applications can serialize these messages with `google.golang.org/protobuf/proto`.
  It also defines `StandaloneDelta` and `BlobDigest` for external-base transfers; these messages are not part of Mononoke's Thrift model.

The protobuf schema represents the same data model, not the same wire format.
Mononoke's current reader does not accept protobuf messages as header-0 envelopes.
The root builder selects and frames either encoding; protobuf uses its distinct library-defined header of `1`.

To regenerate the protobuf Go messages, install `protoc` 32.0 or later for Edition 2024 support and run:

```sh
go generate ./protobuf
```

The generator script builds the `protoc-gen-go` version selected by `go.mod` into a temporary directory.
Generated Go messages are checked in, so normal library builds do not require `protoc`.

## Validation

```sh
go test ./...
go test -race ./...
go vet ./...
```

The Go tests decode protobuf envelopes with the generated messages and Thrift envelopes with Apache Thrift, check the pack hash against an upstream test vector, and verify compression, dictionary chains, standalone transfers, digest checks, decoding bounds, errors, and buffer ownership.

An optional independent check uses Python Apache Thrift and the system `libzstd` to decode Go-generated output, including raw dictionary references at multiple compression levels and standalone deltas:

```sh
uv venv /tmp/packblob-interop
uv pip install --python /tmp/packblob-interop/bin/python thrift==0.22.0
PACKBLOB_INTEROP_PYTHON=/tmp/packblob-interop/bin/python \
    go test -run TestInteroperability -v ./...
```

Install a system `libzstd` if it is not already available.
The independent check skips when `PACKBLOB_INTEROP_PYTHON` is unset.
It checks the public wire format and Zstandard interoperability; it does not run a Mononoke server.
