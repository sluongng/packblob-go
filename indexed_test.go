package packblob_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/sluongng/packblob-go"
)

var indexedTestLimits = packblob.IndexedLimits{MaxEntries: 1024, MaxBlobSize: 1 << 20, MaxEncodedSize: 1 << 20}

type indexedRange struct{ start, end int64 }

type selectiveReaderAt struct {
	data    []byte
	allowed []indexedRange
	reads   []indexedRange
}

func (r *selectiveReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	request := indexedRange{offset, offset + int64(len(p))}
	for _, allowed := range r.allowed {
		if request.start >= allowed.start && request.end <= allowed.end {
			r.reads = append(r.reads, request)
			return bytes.NewReader(r.data).ReadAt(p, offset)
		}
	}
	return 0, errors.New("read touched an unrelated payload or the whole index")
}

func TestIndexedPackSelectiveRead(t *testing.T) {
	base, target := deltaRevisions(t, 64<<10)
	// Force the base to sort after its target. On-disk index ordinals must
	// reference sorted records rather than the original insertion positions.
	for nonce := 0; packblob.SHA256Digest(base).Hash <= packblob.SHA256Digest(target).Hash; nonce++ {
		if nonce == 65536 {
			t.Fatal("could not construct reversed digest order")
		}
		target[len(target)-2], target[len(target)-1] = byte(nonce>>8), byte(nonce)
	}
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	must(t, err)
	baseStart := int64(file.Len())
	baseDigest, err := writer.AddBase(base)
	must(t, err)
	baseEnd := int64(file.Len())
	deltaStart := baseEnd
	targetDigest, err := writer.AddDelta(base, target)
	must(t, err)
	deltaEnd := int64(file.Len())
	unrelated, _ := deltaRevisions(t, 256<<10)
	otherDigest, err := writer.AddBase(unrelated)
	must(t, err)
	indexStart := int64(file.Len())
	must(t, writer.Close())
	contents := file.Bytes()
	size := int64(len(contents))

	// Opening may read only the header and footer, not any payload or index.
	spy := &selectiveReaderAt{data: contents, allowed: []indexedRange{{0, 8}, {size - 32, size}}}
	reader, err := packblob.OpenIndexed(spy, size, indexedTestLimits)
	must(t, err)
	if reader.Len() != 3 {
		t.Fatalf("Len = %d, want 3", reader.Len())
	}
	// Metadata APIs must not read any payloads. Limit each metadata request
	// to one record to rule out eagerly loading the complete index.
	spy.allowed = []indexedRange{{0, 8}, {size - 32, size}}
	for offset := indexStart; offset < size-32; offset += 72 {
		spy.allowed = append(spy.allowed, indexedRange{offset, offset + 72})
	}
	info, err := reader.Lookup(targetDigest)
	must(t, err)
	if info.Digest != targetDigest || info.BaseDigest == nil || *info.BaseDigest != baseDigest || info.Codec != packblob.IndexedDelta || info.Offset != deltaStart || info.EncodedSize != deltaEnd-deltaStart {
		t.Fatalf("wrong application-index metadata: %+v", info)
	}
	for i := int64(0); i < reader.Len(); i++ {
		entry, err := reader.EntryAt(i)
		must(t, err)
		digest, err := reader.DigestAt(i)
		must(t, err)
		if digest != entry.Digest {
			t.Fatal("metadata enumeration disagrees")
		}
	}

	// Serving a negotiated delta reads exactly its own payload, not the base.
	spy.allowed = append(spy.allowed, indexedRange{deltaStart, deltaEnd})
	delta, err := reader.ReadDelta(targetDigest)
	must(t, err)
	wire, err := delta.MarshalBinary()
	must(t, err)
	parsed, err := packblob.UnmarshalDelta(wire, int64(len(wire)))
	must(t, err)
	decoded, err := packblob.DecodeDelta(base, parsed, targetDigest, int64(len(target)))
	must(t, err)
	if !bytes.Equal(decoded, target) {
		t.Fatal("delta-only transfer produced wrong target")
	}

	// A complete extraction may additionally read the base, but never the
	// unrelated 256 KiB version in the same file.
	spy.allowed = append(spy.allowed, indexedRange{baseStart, baseEnd})
	decoded, err = reader.ReadVersion(targetDigest)
	must(t, err)
	if !bytes.Equal(decoded, target) {
		t.Fatal("single-version extraction produced wrong target")
	}
	if delta, err := reader.ReadDelta(baseDigest); !errors.Is(err, packblob.ErrNotDelta) || delta != nil {
		t.Fatalf("base as delta = %v, %v", delta, err)
	}

	// Independent reads and the missing-base transfer path use the same API.
	reader, err = packblob.OpenIndexed(bytes.NewReader(contents), size, indexedTestLimits)
	must(t, err)
	for _, version := range []struct {
		digest packblob.Digest
		data   []byte
	}{{baseDigest, base}, {targetDigest, target}, {otherDigest, unrelated}} {
		got, err := reader.ReadVersion(version.digest)
		must(t, err)
		if !bytes.Equal(got, version.data) {
			t.Fatal("version contents differ")
		}
	}
}

func TestIndexedPackBoundariesAndConcurrentReads(t *testing.T) {
	for _, tc := range []struct {
		name         string
		base, target []byte
	}{
		{name: "empty base", target: []byte("new contents")},
		{name: "empty target", base: []byte("old contents")},
		{name: "identical contents", base: []byte("unchanged"), target: []byte("unchanged")},
		{name: "dictionary magic raw base", base: []byte{0x37, 0xa4, 0x30, 0xec, 0, 0, 0, 0}, target: []byte{0x37, 0xa4, 0x30, 0xec, 0, 0, 0, 1}},
		{name: "compressed base", base: bytes.Repeat([]byte("same line\n"), 8192), target: bytes.Repeat([]byte("same line\n"), 8193)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var file bytes.Buffer
			writer, err := packblob.NewIndexedWriter(&file, 0)
			must(t, err)
			_, err = writer.AddBase(tc.base)
			must(t, err)
			digest, err := writer.AddDelta(tc.base, tc.target)
			must(t, err)
			must(t, writer.Close())
			reader, err := packblob.OpenIndexed(bytes.NewReader(file.Bytes()), int64(file.Len()), indexedTestLimits)
			must(t, err)
			var group sync.WaitGroup
			for range 4 {
				group.Go(func() {
					got, err := reader.ReadVersion(digest)
					if err != nil || !bytes.Equal(got, tc.target) {
						t.Errorf("concurrent extraction = %d bytes, %v", len(got), err)
					}
				})
			}
			group.Wait()
		})
	}
}

func TestIndexedWriterLifecycle(t *testing.T) {
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	must(t, err)
	if err := writer.Close(); !errors.Is(err, packblob.ErrEmptyPack) {
		t.Fatalf("empty Close = %v", err)
	}
	base, target := []byte("base"), []byte("target")
	if _, err := writer.AddDelta(base, target); !errors.Is(err, packblob.ErrInvalidBase) {
		t.Fatalf("unstored base = %v", err)
	}
	_, err = writer.AddBase(base)
	must(t, err)
	digest, err := writer.AddDelta(base, target)
	must(t, err)
	length := file.Len()
	duplicate, err := writer.AddBase(target)
	must(t, err)
	if duplicate != digest || file.Len() != length {
		t.Fatal("duplicate version wrote another payload")
	}
	if _, err := writer.AddDelta(target, []byte("third revision")); !errors.Is(err, packblob.ErrInvalidBase) {
		t.Fatalf("deduplicated delta used as base = %v", err)
	}
	must(t, writer.Close())
	length = file.Len()
	must(t, writer.Close())
	if file.Len() != length {
		t.Fatal("second Close wrote another index")
	}
	if _, err := writer.AddBase(nil); !errors.Is(err, packblob.ErrClosed) {
		t.Fatalf("add after Close = %v", err)
	}

	// A short or failed write is sticky, including finalization failures.
	for _, tc := range []struct {
		name  string
		limit int
		short bool
	}{
		{name: "payload error", limit: 8},
		{name: "payload short write", limit: 8, short: true},
		{name: "index error", limit: 8 + len(base)},
		{name: "footer error", limit: 8 + len(base) + 72},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &limitedIndexedWriter{remaining: tc.limit, short: tc.short}
			writer, err := packblob.NewIndexedWriter(sink, 0)
			must(t, err)
			_, first := writer.AddBase(base)
			if first == nil {
				first = writer.Close()
			}
			if first == nil {
				t.Fatal("write failure was not reported")
			}
			if err := writer.Close(); err != first {
				t.Fatalf("Close lost sticky write error: %v != %v", err, first)
			}
			if _, err := writer.AddBase(nil); err != first {
				t.Fatalf("AddBase lost sticky write error: %v != %v", err, first)
			}
		})
	}
}

type limitedIndexedWriter struct {
	remaining int
	short     bool
}

func (w *limitedIndexedWriter) Write(p []byte) (int, error) {
	if len(p) <= w.remaining {
		w.remaining -= len(p)
		return len(p), nil
	}
	n := w.remaining
	w.remaining = 0
	if w.short {
		return n, nil
	}
	return n, io.ErrClosedPipe
}

func TestIndexedPackRejectsCorruptionAndLimits(t *testing.T) {
	base, target := deltaRevisions(t, 4096)
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	must(t, err)
	baseDigest, err := writer.AddBase(base)
	must(t, err)
	targetDigest, err := writer.AddDelta(base, target)
	must(t, err)
	must(t, writer.Close())
	original := file.Bytes()
	indexOffset := int(binary.LittleEndian.Uint64(original[len(original)-24:]))
	baseOrdinal, targetOrdinal := 0, 1
	if baseDigest.Hash > targetDigest.Hash {
		baseOrdinal, targetOrdinal = 1, 0
	}
	baseRecord := indexOffset + baseOrdinal*72
	targetRecord := indexOffset + targetOrdinal*72

	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "unknown header", mutate: func(p []byte) { p[0] ^= 1 }},
		{name: "huge index count", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[len(p)-16:], math.MaxUint64) }},
		{name: "overflowing index offset", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[len(p)-24:], math.MaxUint64) }},
		{name: "unknown codec", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[targetRecord+64:], 99) }},
		{name: "payload points into index", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[targetRecord+40:], uint64(indexOffset)) }},
		{name: "overflowing payload length", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[targetRecord+48:], math.MaxUint64) }},
		{name: "self reference", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[targetRecord+56:], uint64(targetOrdinal)) }},
		{name: "out of range base", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[targetRecord+56:], 2) }},
		{name: "delta chain", mutate: func(p []byte) {
			binary.LittleEndian.PutUint64(p[baseRecord+64:], 2)
			binary.LittleEndian.PutUint64(p[baseRecord+56:], uint64(targetOrdinal))
		}},
		{name: "anchor has invalid base reference", mutate: func(p []byte) { binary.LittleEndian.PutUint64(p[baseRecord+56:], 0) }},
		{name: "corrupt raw anchor", mutate: func(p []byte) { p[8] ^= 1 }},
		{name: "corrupt delta", mutate: func(p []byte) {
			offset := binary.LittleEndian.Uint64(p[targetRecord+40:])
			p[offset] ^= 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contents := bytes.Clone(original)
			tc.mutate(contents)
			reader, err := packblob.OpenIndexed(bytes.NewReader(contents), int64(len(contents)), indexedTestLimits)
			var output []byte
			if err == nil {
				output, err = reader.ReadVersion(targetDigest)
			}
			if err == nil || output != nil {
				t.Fatalf("corrupt pack accepted: %d bytes, %v", len(output), err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		limits packblob.IndexedLimits
	}{
		{name: "entry count", limits: packblob.IndexedLimits{MaxEntries: 1, MaxBlobSize: 1 << 20, MaxEncodedSize: 1 << 20}},
		{name: "anchor decoded size", limits: packblob.IndexedLimits{MaxEntries: 10, MaxBlobSize: 4095, MaxEncodedSize: 1 << 20}},
		{name: "anchor encoded size", limits: packblob.IndexedLimits{MaxEntries: 10, MaxBlobSize: 1 << 20, MaxEncodedSize: 4095}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := packblob.OpenIndexed(bytes.NewReader(original), int64(len(original)), tc.limits)
			var output []byte
			if err == nil {
				output, err = reader.ReadVersion(targetDigest)
			}
			if !errors.Is(err, packblob.ErrTooLarge) || output != nil {
				t.Fatalf("limit not enforced: %d bytes, %v", len(output), err)
			}
		})
	}
	for _, truncated := range [][]byte{nil, original[:7], original[:len(original)-1], original[:indexOffset+71]} {
		if reader, err := packblob.OpenIndexed(bytes.NewReader(truncated), int64(len(truncated)), indexedTestLimits); err == nil || reader != nil {
			t.Fatal("truncated pack accepted")
		}
	}
	reader, err := packblob.OpenIndexed(bytes.NewReader(original), int64(len(original)), indexedTestLimits)
	must(t, err)
	if output, err := reader.ReadVersion(packblob.SHA256Digest([]byte("absent"))); !errors.Is(err, packblob.ErrVersionNotFound) || output != nil {
		t.Fatalf("missing version = %v, %v", output, err)
	}
	if _, err := reader.EntryAt(reader.Len()); err == nil {
		t.Fatal("out-of-range ordinal accepted")
	}
}

func TestIndexedPackLimitsOnlySelectedPayloads(t *testing.T) {
	large, _ := deltaRevisions(t, 4096)
	small := []byte("x")
	// With two sorted records, binary search probes ordinal 1 first. Put the
	// unrelated large blob there to exercise a real non-selected midpoint.
	for nonce := 0; packblob.SHA256Digest(large).Hash < packblob.SHA256Digest(small).Hash; nonce++ {
		if nonce == 65536 {
			t.Fatal("could not construct search order")
		}
		large[0], large[1] = byte(nonce>>8), byte(nonce)
	}
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	must(t, err)
	smallDigest, err := writer.AddBase(small)
	must(t, err)
	_, err = writer.AddBase(large)
	must(t, err)
	must(t, writer.Close())
	reader, err := packblob.OpenIndexed(bytes.NewReader(file.Bytes()), int64(file.Len()), packblob.IndexedLimits{MaxEntries: 10, MaxBlobSize: 64, MaxEncodedSize: 64})
	must(t, err)
	_, err = reader.Lookup(smallDigest)
	must(t, err)
	got, err := reader.ReadVersion(smallDigest)
	must(t, err)
	if !bytes.Equal(got, small) {
		t.Fatal("small version extraction differs")
	}

	// Conversely, a small delta really does require its large anchor. Reject
	// that anchor under either cap before trying to read a payload.
	file.Reset()
	writer, err = packblob.NewIndexedWriter(&file, 0)
	must(t, err)
	_, err = writer.AddBase(large)
	must(t, err)
	deltaStart := int64(file.Len())
	smallDigest, err = writer.AddDelta(large, small)
	must(t, err)
	indexStart := int64(file.Len())
	must(t, writer.Close())
	for _, limits := range []packblob.IndexedLimits{
		{MaxEntries: 10, MaxBlobSize: 64, MaxEncodedSize: 1 << 20},
		{MaxEntries: 10, MaxBlobSize: 1 << 20, MaxEncodedSize: 64},
	} {
		spy := &selectiveReaderAt{data: file.Bytes(), allowed: []indexedRange{{0, 8}, {indexStart, int64(file.Len())}}}
		reader, err := packblob.OpenIndexed(spy, int64(file.Len()), limits)
		must(t, err)
		info, err := reader.Lookup(smallDigest)
		must(t, err)
		if info.BaseDigest == nil || *info.BaseDigest != packblob.SHA256Digest(large) {
			t.Fatal("metadata lookup lost large base identity")
		}
		for index := int64(0); index < reader.Len(); index++ {
			_, err := reader.EntryAt(index)
			must(t, err)
		}
		if got, err := reader.ReadVersion(smallDigest); !errors.Is(err, packblob.ErrTooLarge) || got != nil {
			t.Fatalf("oversized anchor = %d bytes, %v", len(got), err)
		}
		// Negotiated delta serving does not reconstruct or read that anchor.
		spy.allowed = append(spy.allowed, indexedRange{deltaStart, indexStart})
		delta, err := reader.ReadDelta(smallDigest)
		must(t, err)
		got, err := packblob.DecodeDelta(large, delta, smallDigest, 64)
		must(t, err)
		if !bytes.Equal(got, small) {
			t.Fatal("delta transfer with large client base differs")
		}
	}
}

type indexedReaderAtFunc func([]byte, int64) (int, error)

func (f indexedReaderAtFunc) ReadAt(p []byte, offset int64) (int, error) { return f(p, offset) }

func TestIndexedReaderAtContract(t *testing.T) {
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	must(t, err)
	digest, err := writer.AddBase([]byte("contents"))
	must(t, err)
	must(t, writer.Close())
	backing := bytes.NewReader(file.Bytes())
	fullEOF := indexedReaderAtFunc(func(p []byte, offset int64) (int, error) {
		n, err := backing.ReadAt(p, offset)
		if n == len(p) && offset+int64(n) == int64(file.Len()) {
			return n, io.EOF // A conforming ReaderAt may report EOF here.
		}
		return n, err
	})
	reader, err := packblob.OpenIndexed(fullEOF, int64(file.Len()), indexedTestLimits)
	must(t, err)
	_, err = reader.ReadVersion(digest)
	must(t, err)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "short read with EOF", err: io.EOF},
		{name: "broken short read without error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			short := indexedReaderAtFunc(func(p []byte, offset int64) (int, error) {
				n, _ := backing.ReadAt(p[:len(p)-1], offset)
				return n, tc.err
			})
			if reader, err := packblob.OpenIndexed(short, int64(file.Len()), indexedTestLimits); err == nil || reader != nil {
				t.Fatal("short ReaderAt accepted")
			}
		})
	}
}

type countingIndexedReaderAt struct {
	io.ReaderAt
	bytes int64
}

func (r *countingIndexedReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(p, offset)
	r.bytes += int64(n)
	return n, err
}

func BenchmarkIndexedVersionRead(b *testing.B) {
	base, target := deltaRevisions(b, 64<<10)
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := writer.AddBase(base); err != nil {
		b.Fatal(err)
	}
	var digest packblob.Digest
	for revision := range 32 {
		target[len(target)-1] = byte(revision)
		digest, err = writer.AddDelta(base, target)
		if err != nil {
			b.Fatal(err)
		}
	}
	unrelated, _ := deltaRevisions(b, 256<<10)
	if _, err := writer.AddBase(unrelated); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	for _, operation := range []string{"lookup", "delta", "version"} {
		b.Run(operation, func(b *testing.B) {
			input := &countingIndexedReaderAt{ReaderAt: bytes.NewReader(file.Bytes())}
			reader, err := packblob.OpenIndexed(input, int64(file.Len()), indexedTestLimits)
			if err != nil {
				b.Fatal(err)
			}
			input.bytes = 0
			b.ReportAllocs()
			for b.Loop() {
				switch operation {
				case "lookup":
					_, err = reader.Lookup(digest)
				case "delta":
					_, err = reader.ReadDelta(digest)
				case "version":
					_, err = reader.ReadVersion(digest)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(input.bytes)/float64(b.N), "read-bytes/op")
			b.ReportMetric(float64(file.Len()), "pack-bytes")
		})
	}
}
