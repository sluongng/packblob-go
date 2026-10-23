# Protobuf file and delta transfer

This example uploads a file to a small HTTP server using protobuf request and response bodies.
The server stores verified files in memory, addressed by SHA-256 hash and uncompressed size.
The [upload schema](protobuf/transfer.proto) uses a `oneof` to carry either the full file or a [`StandaloneDelta`](../../protobuf/packblob.proto).

## Run it

Run these commands from the repository root.
Normal builds use the checked-in generated Go messages and do not require `protoc`.

In one terminal, start the server:

```sh
go run ./examples/transfer/cmd/server -listen 127.0.0.1:8090
```

In another terminal, send the two checked-in text files in order:

```sh
go run ./examples/transfer/cmd/client \
    -server http://127.0.0.1:8090 \
    examples/transfer/testdata/first.txt \
    examples/transfer/testdata/second.txt
```

The client sends `first.txt` as a full file.
After each successful upload, it uses that file as the candidate base for the next file.
The second upload finds `first.txt` on the server and sends only the delta and its metadata for `second.txt`.
The files contain different text, with a few edits between versions.
The client reports each filename, transfer mode, digest, and upload bytes.
The server logs each accepted upload's verified SHA-256 checksum, size, and transfer mode.
For small files or unrelated contents, the client may choose a full-file upload instead.

### Verified two-file run

The checked-in text files produced these results in a local run:

| File | Transfer | File size | Upload body | Server checksum |
| --- | --- | ---: | ---: | --- |
| `first.txt` | Full file | 2,764 bytes | 2,838 bytes | Passed |
| `second.txt` | Delta against first | 2,777 bytes | 394 bytes | Passed |

The upload body includes protobuf metadata, but excludes HTTP headers, responses, and the `FindMissing` request.
Both server-reported SHA-256 hashes matched local `sha256sum` output.
To compare the hashes with the server's verification logs, run:

```sh
sha256sum examples/transfer/testdata/first.txt examples/transfer/testdata/second.txt
```

### Try a missing base

To try the missing-base path, stop and restart the server, then provide a local base without first uploading it:

```sh
go run ./examples/transfer/cmd/client \
    -server http://127.0.0.1:8090 \
    -base examples/transfer/testdata/first.txt \
    examples/transfer/testdata/second.txt
```

The restarted server has no base, so the client sends the full `second.txt` file without uploading `first.txt` first.

## Request flow

1. When a candidate base is available, from `-base` or the preceding upload, the client hashes it and calls `FindMissing`.
2. If the server has that digest, the client creates a delta and compares the complete serialized delta upload request with a full-file upload request.
3. The client uploads the delta only when it is smaller; otherwise it uploads the full target file.
4. The server reconstructs a delta using its local base, or uses the received full-file bytes.
5. The server checks the target's SHA-256 hash and size before storing it under its digest.

`FindMissing` is an availability check, not a reservation.
If a delta upload reaches a server that no longer has its base, the server returns HTTP `409 Conflict` and the client retries once with the full target.
Malformed data and digest mismatches are rejected, not retried as full-file uploads.

## Protocol

All requests use HTTP `POST` with protobuf bodies.
The schema is specific to this example; it is not gRPC or a Bazel Remote Execution API implementation.

| Endpoint | Request | Response |
| --- | --- | --- |
| `/findmissing` | `FindMissingRequest`: digests to check | `FindMissingResponse`: digests absent from the server |
| `/upload` | `UploadRequest`: target digest and a `oneof` containing full-file bytes or a standalone delta | `UploadResponse`: stored target digest |

To regenerate the example's Go messages after changing the schema, run `go generate ./examples/transfer/protobuf` with the `protoc` version described in the [root README](../../README.md#schema-packages).

The outer upload request supplies the expected target digest independently of the delta's embedded metadata.
The server checks that both agree and that the reconstructed bytes match.
This protects content integrity; it does not establish who authorized the target digest.
An authenticated application must obtain the expected identity from its own trusted request context.

## Scope

This is an in-memory demonstration, not a production blobstore.
It defaults to loopback and has no authentication, TLS, persistence, eviction, or total-storage quota.
Files are limited to 4 MiB, and protobuf request and response bodies are bounded.
Both sides load each file into memory.
Restarting the server loses all uploaded files.
