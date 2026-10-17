# packblob-go

`packblob` is a pure-Go writer for Mononoke's packblob storage format.
It combines related blobs into an ordered pack, using earlier blobs as Zstandard dictionaries.
It returns the encoded bytes and storage keys without accessing a blobstore.

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

The zero value of `Builder` also uses default compression.
`AddBase` always emits a standalone Zstandard frame, even for empty data.
`AddDelta` always emits a dictionary-compressed entry and requires its dictionary key to exist already.
A delta can itself serve as a dictionary for a later entry.
Duplicate keys and forward references are rejected without changing the builder.

`Build` returns:

- `Key`: the pack's storage identity, including your prefix and `.pack` suffix.
- `LinkKeys`: sorted logical entry keys, without repository prefixes or `.pack` suffixes.
- `Data`: the complete storage envelope, including the four-byte format header.

For Mononoke's underlying storage layout, write `Data` under `Key`, then create a hardlink-like copy for each `<repository-prefix><logical-key>.pack`.
Mononoke's own writer removes the temporary pack key after those copies succeed.
Storage writes, copy semantics, error recovery, and removal are the caller's responsibility.
The pack prefix passed to `Build` is independent of the repository prefix applied to links.
Pass unprefixed keys to `AddBase` and `AddDelta`; recognized `repoNNNN.` and `ephN.repoNNNN.` prefixes are rejected.

## Independent blobs

Use `EncodeSingle(data, level)` to create a standalone envelope.
It stores the payload as Zstandard only when compression makes it strictly smaller; otherwise it stores raw bytes.
Use `EncodeRaw(data)` to force raw storage.
Store either result at the logical blob key plus `packblob.EnvelopeSuffix` (`.pack`).

## Compatibility and limits

The format follows the upstream [Thrift schema](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/mononoke/blobstore/packblob/if/packblob.thrift), [envelope framing](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/mononoke/blobstore/packblob/src/envelope.rs), and [pack writer](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/mononoke/blobstore/packblob/src/pack.rs).

- Envelopes contain a four-byte big-endian zero followed by a Thrift Compact Protocol `StorageEnvelope`.
- Pack identities use keyed BLAKE2b-256 with key `pack`, hashing sorted logical keys as an eight-byte little-endian UTF-8 byte length followed by the key bytes.
  This matches 64-bit Mononoke, whose Rust implementation encodes a native `usize` length.
- **Keys must identify immutable contents.** Pack identity depends only on the logical keys, not the payloads, entry order, or compression settings.
  Reusing a key for different contents can collide with an existing pack identity.
- Compression level `0` means the default, `3`.
  Other levels from `-131072` through `22` map to the pure-Go encoder's nearest preset.
  The output is decodable by libzstd but is not expected to match libzstd byte for byte or achieve the same compression ratio.
- Each builder retains raw dictionary bytes and compressed payloads for all entries.
  `Build` allocates another buffer for the envelope.
  Bound pack sizes in the caller; this is not a streaming API.
- Builders copy input data, and each `Build` returns independent slices.
  A builder can be extended after building but must not be copied after use or used concurrently.
- A blob of at least eight bytes starting with Zstandard dictionary magic (`37 a4 30 ec`) cannot be used as a raw dictionary.
  `AddDelta` returns `ErrInvalidDictionary` because Mononoke's decoder would interpret those bytes as a formatted dictionary.
  The blob can still be stored as a base or standalone value.
- Keys and prefixes must be valid UTF-8.
  Individual strings, payloads, and entry counts are limited by Thrift's signed 32-bit lengths.
- This package creates envelopes only; it does not implement reading, automatic dictionary selection, blobstore access, or a command-line tool.

## Schema packages

The module is `github.com/sluongng/packblob-go`; its root Go package is `packblob`.

- [`thrift`](thrift) contains the Mononoke-compatible [schema](thrift/packblob.thrift) and Compact Protocol encoder.
  Its `MarshalSingle` and `MarshalPack` functions wrap already-compressed values in the four-byte-header storage envelope.
  Use the root package for compression, dictionary ordering, and pack-key creation.
- [`protobuf`](protobuf) contains an equivalent [Protocol Buffers schema](protobuf/packblob.proto) and generated Go messages.
  Its `oneof` fields represent the Thrift unions, and its repeated entries preserve dictionary order.
  Edition 2024 provides explicit scalar presence and Go's Opaque API: use setters or generated builders to construct messages, and `Has` methods to distinguish missing keys from explicit empty keys.
  Applications can serialize these messages with `google.golang.org/protobuf/proto`.

The protobuf schema represents the same data model, not the same wire format.
Mononoke's current reader does not accept protobuf messages as header-0 envelopes.
The root builder continues to use Thrift; no protobuf storage header is assigned here.

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

The Go tests decode envelopes with Apache Thrift, check the pack hash against an upstream test vector, and verify compression, dictionary chains, errors, and buffer ownership.

An optional independent check uses Python Apache Thrift and the system `libzstd` to decode Go-generated output, including raw dictionary references at multiple compression levels:

```sh
uv venv /tmp/packblob-interop
uv pip install --python /tmp/packblob-interop/bin/python thrift==0.22.0
PACKBLOB_INTEROP_PYTHON=/tmp/packblob-interop/bin/python \
    go test -run TestInteroperability -v ./...
```

Install a system `libzstd` if it is not already available.
The independent check skips when `PACKBLOB_INTEROP_PYTHON` is unset.
It checks the public wire format and Zstandard interoperability; it does not run a Mononoke server.
