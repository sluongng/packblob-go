package packblob_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/sluongng/packblob-go"
	packblobpb "github.com/sluongng/packblob-go/protobuf"
	"google.golang.org/protobuf/proto"
)

func TestStandaloneDeltaTransfer(t *testing.T) {
	base, target := deltaRevisions(t, 64<<10)
	for _, tc := range []struct {
		name           string
		base, target   []byte
		requireSavings bool
	}{
		{name: "shared high entropy base", base: base, target: target, requireSavings: true},
		{name: "raw base begins with dictionary magic", base: append([]byte{0x37, 0xa4, 0x30, 0xec}, base...), target: target},
		{name: "empty base", target: []byte("new contents")},
		{name: "empty target", base: []byte("old contents")},
		{name: "both empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta, err := packblob.EncodeDelta(tc.base, tc.target, 0)
			must(t, err)
			if delta.BaseDigest != packblob.SHA256Digest(tc.base) || delta.TargetDigest != packblob.SHA256Digest(tc.target) {
				t.Fatal("packet content identities differ")
			}
			wire, err := delta.MarshalBinary()
			must(t, err)
			// The transport carries only this standalone message. The receiver
			// already has tc.base and the expected target digest from its request.
			var message packblobpb.StandaloneDelta
			must(t, proto.Unmarshal(wire, &message))
			if message.GetBaseDigest().GetHash() != delta.BaseDigest.Hash || !bytes.Equal(message.GetZstd(), delta.Zstd) {
				t.Fatal("standalone protobuf descriptor changed")
			}
			if tc.requireSavings {
				full, err := packblob.EncodeSingle(tc.target, 0)
				must(t, err)
				if len(wire) >= len(full) || bytes.Contains(wire, tc.base) {
					t.Fatalf("delta packet did not avoid base transfer: delta %d bytes, full %d bytes", len(wire), len(full))
				}
			}
			received, err := packblob.UnmarshalDelta(wire, int64(len(wire)))
			must(t, err)
			decoded, err := packblob.DecodeDelta(tc.base, received, packblob.SHA256Digest(tc.target), int64(len(tc.target)))
			must(t, err)
			if !bytes.Equal(decoded, tc.target) {
				t.Fatal("reconstructed target differs")
			}
		})
	}
}

func TestStandaloneDeltaRejectsInvalidInputs(t *testing.T) {
	base, target := deltaRevisions(t, 4096)
	original, err := packblob.EncodeDelta(base, target, 0)
	must(t, err)
	expected := packblob.SHA256Digest(target)
	wrongBase := bytes.Clone(base)
	wrongBase[0] ^= 1
	for _, tc := range []struct {
		name     string
		base     []byte
		mutate   func(*packblob.Delta)
		expected packblob.Digest
		limit    int64
		want     error
	}{
		{name: "wrong base", base: wrongBase, expected: expected, limit: int64(len(target)), want: packblob.ErrBaseMismatch},
		{name: "untrusted target cannot replace request identity", base: base, expected: packblob.SHA256Digest([]byte("another target")), limit: int64(len(target)), want: packblob.ErrTargetMismatch},
		{name: "output exceeds caller limit", base: base, expected: expected, limit: int64(len(target) - 1), want: packblob.ErrTooLarge},
		{name: "negative output limit", base: base, expected: expected, limit: -1, want: packblob.ErrTooLarge},
		{name: "unsupported digest function", base: base, expected: expected, limit: int64(len(target)), mutate: func(d *packblob.Delta) { d.DigestFunction = "unsupported" }, want: packblob.ErrUnsupportedDigestFunction},
		{name: "invalid base hash", base: base, expected: expected, limit: int64(len(target)), mutate: func(d *packblob.Delta) { d.BaseDigest.Hash = "invalid" }, want: packblob.ErrInvalidDelta},
		{name: "negative target size", base: base, expected: expected, limit: int64(len(target)), mutate: func(d *packblob.Delta) { d.TargetDigest.SizeBytes = -1 }, want: packblob.ErrInvalidDelta},
		{name: "corrupt frame magic", base: base, expected: expected, limit: int64(len(target)), mutate: func(d *packblob.Delta) { d.Zstd[0] ^= 1 }, want: packblob.ErrCorruptDelta},
		{name: "truncated frame", base: base, expected: expected, limit: int64(len(target)), mutate: func(d *packblob.Delta) { d.Zstd = d.Zstd[:len(d.Zstd)-1] }, want: packblob.ErrCorruptDelta},
		{name: "decoded target hash mismatch", base: base, expected: packblob.SHA256Digest(bytes.Repeat([]byte{42}, len(target))), limit: int64(len(target)), mutate: func(d *packblob.Delta) { d.TargetDigest = packblob.SHA256Digest(bytes.Repeat([]byte{42}, len(target))) }, want: packblob.ErrTargetMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta := *original
			delta.Zstd = bytes.Clone(original.Zstd)
			if tc.mutate != nil {
				tc.mutate(&delta)
			}
			output, err := packblob.DecodeDelta(tc.base, &delta, tc.expected, tc.limit)
			if !errors.Is(err, tc.want) || output != nil {
				t.Fatalf("DecodeDelta = %d bytes, %v; want nil, %v", len(output), err, tc.want)
			}
		})
	}
	if output, err := packblob.DecodeDelta(base, nil, expected, int64(len(target))); !errors.Is(err, packblob.ErrInvalidDelta) || output != nil {
		t.Fatalf("nil packet = %v, %v", output, err)
	}
	if _, err := packblob.EncodeDelta(nil, nil, 23); !errors.Is(err, packblob.ErrInvalidLevel) {
		t.Fatalf("invalid compression level = %v", err)
	}
}

func TestStandaloneDeltaBoundsUnknownSizeFrames(t *testing.T) {
	// Streaming frames omit the target size. Output bounds must still apply
	// while decoding rather than only comparing the result afterward.
	base := []byte("shared base")
	var frame bytes.Buffer
	encoder, err := zstd.NewWriter(&frame, zstd.WithEncoderConcurrency(1), zstd.WithEncoderDictRaw(0, base))
	must(t, err)
	_, err = encoder.Write(bytes.Repeat([]byte("large output"), 8192))
	must(t, err)
	must(t, encoder.Close())
	expected := packblob.SHA256Digest([]byte("x"))
	delta := &packblob.Delta{BaseDigest: packblob.SHA256Digest(base), TargetDigest: expected, DigestFunction: packblob.DigestFunctionSHA256, Zstd: frame.Bytes()}
	if output, err := packblob.DecodeDelta(base, delta, expected, 1); !errors.Is(err, packblob.ErrTooLarge) || output != nil {
		t.Fatalf("oversized frame = %d bytes, %v", len(output), err)
	}
	// A skippable frame is not a valid standalone delta, even for an empty target.
	delta.TargetDigest = packblob.SHA256Digest(nil)
	delta.Zstd = []byte{0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0}
	if output, err := packblob.DecodeDelta(base, delta, delta.TargetDigest, 0); !errors.Is(err, packblob.ErrCorruptDelta) || output != nil {
		t.Fatalf("skippable frame = %v, %v", output, err)
	}
}

func TestStandaloneDeltaWireValidationAndOwnership(t *testing.T) {
	base, target := deltaRevisions(t, 4096)
	savedBase, savedTarget := bytes.Clone(base), bytes.Clone(target)
	delta, err := packblob.EncodeDelta(base, target, 0)
	must(t, err)
	clear(base)
	clear(target)
	wire, err := delta.MarshalBinary()
	must(t, err)
	if output, err := packblob.UnmarshalDelta(wire, int64(len(wire)-1)); !errors.Is(err, packblob.ErrTooLarge) || output != nil {
		t.Fatalf("encoded size limit = %v, %v", output, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*packblobpb.StandaloneDelta)
		want   error
	}{
		{name: "missing base descriptor", mutate: func(m *packblobpb.StandaloneDelta) { m.ClearBaseDigest() }, want: packblob.ErrInvalidDelta},
		{name: "missing size presence", mutate: func(m *packblobpb.StandaloneDelta) { m.GetBaseDigest().ClearSizeBytes() }, want: packblob.ErrInvalidDelta},
		{name: "missing compressed data", mutate: func(m *packblobpb.StandaloneDelta) { m.ClearZstd() }, want: packblob.ErrInvalidDelta},
		{name: "unsupported digest function", mutate: func(m *packblobpb.StandaloneDelta) { m.SetDigestFunction("unsupported") }, want: packblob.ErrUnsupportedDigestFunction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var message packblobpb.StandaloneDelta
			must(t, proto.Unmarshal(wire, &message))
			tc.mutate(&message)
			malformed, err := proto.Marshal(&message)
			must(t, err)
			if packet, err := packblob.UnmarshalDelta(malformed, int64(len(malformed))); !errors.Is(err, tc.want) || packet != nil {
				t.Fatalf("UnmarshalDelta = %v, %v; want nil, %v", packet, err, tc.want)
			}
		})
	}
	for _, malformed := range [][]byte{nil, {0xff}, wire[:len(wire)-1], append(bytes.Clone(wire), 0x98, 0x06, 0x01)} {
		if packet, err := packblob.UnmarshalDelta(malformed, int64(len(malformed))); !errors.Is(err, packblob.ErrInvalidDelta) || packet != nil {
			t.Fatalf("malformed packet = %v, %v", packet, err)
		}
	}
	parsed, err := packblob.UnmarshalDelta(wire, int64(len(wire)))
	must(t, err)
	clear(wire)
	clear(delta.Zstd)
	decoded, err := packblob.DecodeDelta(savedBase, parsed, packblob.SHA256Digest(savedTarget), int64(len(savedTarget)))
	must(t, err)
	if !bytes.Equal(decoded, savedTarget) {
		t.Fatal("input mutation changed packet contents")
	}
}

func BenchmarkStandaloneDelta(b *testing.B) {
	for _, size := range []int{1024, 16 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("%dKiB", size/1024), func(b *testing.B) {
			base, target := deltaRevisions(b, size)
			full, err := packblob.EncodeSingle(target, 3)
			if err != nil {
				b.Fatal(err)
			}
			delta, err := packblob.EncodeDelta(base, target, 3)
			if err != nil {
				b.Fatal(err)
			}
			wire, err := delta.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				delta, err := packblob.EncodeDelta(base, target, 3)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := delta.MarshalBinary(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(wire)), "delta-wire-bytes")
			b.ReportMetric(float64(len(full)), "full-wire-bytes")
		})
	}
}

func deltaRevisions(t testing.TB, size int) ([]byte, []byte) {
	t.Helper()
	random := rand.New(rand.NewSource(42))
	base := make([]byte, size)
	if _, err := random.Read(base); err != nil {
		t.Fatal(err)
	}
	target := bytes.Clone(base)
	changed := max(1, size/64)
	if _, err := random.Read(target[len(target)-changed:]); err != nil {
		t.Fatal(err)
	}
	return base, target
}
