#!/bin/sh
# Use the same protoc-gen-go version as the library schema.
set -eu
cd "$(dirname "$0")/../../.."
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
go build -o "$tmp_dir/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go
protoc --plugin="protoc-gen-go=$tmp_dir/protoc-gen-go" -I . \
  --go_out=. --go_opt=paths=source_relative examples/transfer/protobuf/transfer.proto
