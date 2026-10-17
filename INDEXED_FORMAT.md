# Indexed blob file format, version 1

This library-specific format supports reading one SHA-256-addressed blob without loading the complete file or index.
It is not a Mononoke pack, protobuf storage envelope, or standalone delta packet.
All integers are unsigned 64-bit little-endian values unless stated otherwise.

## Layout

| Region | Size | Contents |
| --- | --- | --- |
| Header | 8 bytes | ASCII `PBLBIDX1` |
| Payloads | Variable | Independent raw/Zstandard blobs and raw-dictionary Zstandard deltas |
| Index | 72 bytes per record | Records sorted by raw SHA-256 hash, then uncompressed size |
| Footer | 32 bytes | ASCII `PBLBEND1`, index offset, record count, record size (`72`) |

The index offset is measured from the start of the file.
The index ends immediately before the footer.
The footer is written only when the writer closes successfully.
The format has no append protocol or interrupted-write recovery.

## Index record

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 32 bytes | Raw SHA-256 of uncompressed contents |
| 32 | 8 bytes | Uncompressed size |
| 40 | 8 bytes | Payload offset from start of file |
| 48 | 8 bytes | Encoded payload length |
| 56 | 8 bytes | Anchor's zero-based ordinal in the sorted index, or `MaxUint64` for an independent blob |
| 64 | 8 bytes | Codec: `0` raw, `1` Zstandard, `2` raw-dictionary Zstandard delta |

A digest is the pair of hash and uncompressed size.
Each distinct digest has one record, regardless of how many revision labels refer to it.
Records do not preserve insertion or revision order.
An independent record has codec `0` or `1` and no anchor.
A delta record has codec `2` and references an independent record in the same file.
Delta chains are not allowed.
Raw-dictionary compression accepts arbitrary anchor bytes; it does not interpret Zstandard dictionary-magic prefixes as formatted dictionaries.

## Read and storage boundaries

Opening reads the header and footer and checks the index extent against the supplied file size and entry limit.
Version extraction binary-searches individual index records, then reads the requested payload and at most one anchor payload.
Metadata-only lookup and enumeration do not read payloads.
They enforce format bounds without applying payload allocation limits to metadata.
Reading a delta for transfer reads its encoded payload without reading its anchor; the recipient must reconstruct and verify it before trusting the target contents.
The reader's byte limits apply only to the target during delta-only transfer, and to both target and anchor during complete extraction.
The reader checks accessed records; `ReadVersion` verifies decoded hashes and sizes under the caller's encoded and decoded limits.
It does not scan or certify the entire index when opening.
The backing file must remain stable during reads.

The writer streams payloads and keeps metadata in memory until it writes the sorted index.
It keeps the first representation of a duplicate digest.
Callers choose anchors and manage retention, compaction, durability, and atomic publication of completed files.
An application can catalog digests and payload ranges by immutable pack identifier; the file format does not define a cross-pack catalog or transfer protocol.
