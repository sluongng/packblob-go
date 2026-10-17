package packblob

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	indexedHeader     = "PBLBIDX1"
	indexedFooter     = "PBLBEND1"
	indexedRecordSize = 72
	indexedFooterSize = 32
)

const (
	indexedCodecRaw uint64 = iota
	indexedCodecZstd
	indexedCodecDelta
)

// IndexedCodec identifies the representation of a version's payload.
type IndexedCodec uint8

const (
	// IndexedRaw stores the target bytes directly.
	IndexedRaw IndexedCodec = iota
	// IndexedZstd stores an independently compressed Zstandard frame.
	IndexedZstd
	// IndexedDelta stores a Zstandard frame requiring BaseDigest's raw contents.
	IndexedDelta
)

// VersionInfo locates an encoded payload within an immutable indexed pack.
// Applications may store this metadata in a digest-to-pack catalog. The pack
// identity and the lifetime of that catalog remain the application's concern.
// BaseDigest is nil for independent versions and owns a fresh value otherwise.
type VersionInfo struct {
	Digest      Digest
	BaseDigest  *Digest
	Offset      int64
	EncodedSize int64
	Codec       IndexedCodec
}

var (
	ErrInvalidPack     = errors.New("packblob: invalid indexed pack")
	ErrVersionNotFound = errors.New("packblob: version not found")
	ErrInvalidBase     = errors.New("packblob: delta base must be an independent indexed version")
	ErrNotDelta        = errors.New("packblob: indexed version is not a delta")
	ErrClosed          = errors.New("packblob: indexed writer is closed")
)

// IndexedLimits bounds metadata and per-version allocations when reading an
// indexed pack. MaxEntries and MaxEncodedSize must be positive; MaxBlobSize
// may be zero for packs containing only empty blobs. Entry and byte limits
// cannot exceed math.MaxInt32. Byte limits apply to contents read or decoded,
// not metadata-only Lookup or EntryAt. MaxEncodedSize limits each payload,
// not the entire pack file.
type IndexedLimits struct {
	MaxEntries     int64
	MaxBlobSize    int64
	MaxEncodedSize int64
}

type indexedRecord struct {
	digest  Digest
	hash    [32]byte
	offset  uint64
	length  uint64
	base    uint64
	codec   uint64
	anchor  *indexedRecord
	ordinal uint64
}

// IndexedWriter streams a seekable, multi-version pack to an io.Writer. It
// retains only index metadata, not blob contents or compressed payloads.
// Call Close to append the sorted index and footer; Close does not close w.
// A writer is not safe for concurrent calls and must not be copied after use.
type IndexedWriter struct {
	w       io.Writer
	level   int
	offset  int64
	entries map[Digest]*indexedRecord
	err     error
	closed  bool
}

// NewIndexedWriter writes the format header and creates an empty writer.
// The output must start at the beginning of a new file or byte stream.
func NewIndexedWriter(w io.Writer, zstdLevel int) (*IndexedWriter, error) {
	if w == nil {
		return nil, fmt.Errorf("%w: nil writer", ErrInvalidPack)
	}
	if err := validateLevel(zstdLevel); err != nil {
		return nil, err
	}
	writer := &IndexedWriter{w: w, level: zstdLevel, entries: make(map[Digest]*indexedRecord)}
	if err := writer.write([]byte(indexedHeader)); err != nil {
		return nil, err
	}
	return writer, nil
}

// AddBase adds an independently readable version, using Zstandard only when it
// saves space. Identical content is deduplicated. If that content was already
// added as a delta, this call does not promote it to an independent base.
func (w *IndexedWriter) AddBase(data []byte) (Digest, error) {
	if err := w.state(); err != nil {
		return Digest{}, err
	}
	if len(data) > math.MaxInt32 {
		return Digest{}, ErrTooLarge
	}
	digest := SHA256Digest(data)
	if _, ok := w.entries[digest]; ok {
		return digest, nil
	}
	payload, err := compress(data, w.level, nil, false)
	if err != nil {
		return Digest{}, err
	}
	codec := uint64(indexedCodecZstd)
	if len(payload) >= len(data) {
		payload = data
		codec = indexedCodecRaw
	}
	return w.add(digest, payload, codec, nil)
}

// AddDelta adds target using the supplied raw base contents as its dictionary.
// The base must already have been added independently by AddBase; delta chains
// are rejected. Supplying base again avoids retaining its contents in memory.
// Identical targets are deduplicated. Validation and compression errors leave
// the writer usable; an output write failure is permanent.
func (w *IndexedWriter) AddDelta(base, target []byte) (Digest, error) {
	if err := w.state(); err != nil {
		return Digest{}, err
	}
	if len(base) > math.MaxInt32 || len(target) > math.MaxInt32 {
		return Digest{}, ErrTooLarge
	}
	anchor, ok := w.entries[SHA256Digest(base)]
	if !ok || anchor.codec == indexedCodecDelta {
		return Digest{}, ErrInvalidBase
	}
	digest := SHA256Digest(target)
	if _, ok := w.entries[digest]; ok {
		return digest, nil
	}
	payload, err := compress(target, w.level, base, true)
	if err != nil {
		return Digest{}, err
	}
	return w.add(digest, payload, indexedCodecDelta, anchor)
}

func (w *IndexedWriter) state() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return ErrClosed
	}
	return nil
}

func (w *IndexedWriter) add(digest Digest, payload []byte, codec uint64, anchor *indexedRecord) (Digest, error) {
	// Reserve room for the eventual index and footer before writing any payload.
	count := int64(len(w.entries)) + 1
	if count > math.MaxInt32 {
		return Digest{}, ErrTooLarge
	}
	if count > (math.MaxInt64-indexedFooterSize)/indexedRecordSize ||
		int64(len(payload)) > math.MaxInt64-w.offset-count*indexedRecordSize-indexedFooterSize {
		return Digest{}, ErrTooLarge
	}
	record := &indexedRecord{digest: digest, offset: uint64(w.offset), length: uint64(len(payload)), codec: codec, anchor: anchor, base: math.MaxUint64}
	_, _ = hex.Decode(record.hash[:], []byte(digest.Hash))
	if err := w.write(payload); err != nil {
		return Digest{}, err
	}
	w.entries[digest] = record
	return digest, nil
}

func (w *IndexedWriter) write(data []byte) error {
	n, err := w.w.Write(data)
	if n < 0 || n > len(data) {
		err = fmt.Errorf("packblob: invalid write count %d", n)
		n = 0
	}
	w.offset += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
	}
	return err
}

// Close finalizes the index without closing the underlying writer. After a
// successful Close, repeated calls succeed and Add methods return ErrClosed.
// Closing an empty pack returns ErrEmptyPack and allows subsequent additions.
func (w *IndexedWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return nil
	}
	if len(w.entries) == 0 {
		return ErrEmptyPack
	}
	entries := make([]*indexedRecord, 0, len(w.entries))
	for _, entry := range w.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return compareIndexedDigest(entries[i].digest, entries[j].digest) < 0 })
	for i, entry := range entries {
		entry.ordinal = uint64(i)
	}
	indexOffset := uint64(w.offset)
	var encoded [indexedRecordSize]byte
	for _, entry := range entries {
		copy(encoded[:32], entry.hash[:])
		binary.LittleEndian.PutUint64(encoded[32:40], uint64(entry.digest.SizeBytes))
		binary.LittleEndian.PutUint64(encoded[40:48], entry.offset)
		binary.LittleEndian.PutUint64(encoded[48:56], entry.length)
		base := uint64(math.MaxUint64)
		if entry.anchor != nil {
			base = entry.anchor.ordinal
		}
		binary.LittleEndian.PutUint64(encoded[56:64], base)
		binary.LittleEndian.PutUint64(encoded[64:72], entry.codec)
		if err := w.write(encoded[:]); err != nil {
			return err
		}
	}
	var footer [indexedFooterSize]byte
	copy(footer[:8], indexedFooter)
	binary.LittleEndian.PutUint64(footer[8:16], indexOffset)
	binary.LittleEndian.PutUint64(footer[16:24], uint64(len(entries)))
	binary.LittleEndian.PutUint64(footer[24:32], indexedRecordSize)
	if err := w.write(footer[:]); err != nil {
		return err
	}
	w.closed = true
	return nil
}

// IndexedReader reads individual versions with ReaderAt, without loading the
// index or unrelated payloads. The underlying file must remain immutable.
// Calls may run concurrently if the supplied ReaderAt supports concurrent use.
type IndexedReader struct {
	r           io.ReaderAt
	indexOffset int64
	count       int64
	limits      IndexedLimits
}

// OpenIndexed reads only the header and footer. Index records and payloads are
// validated lazily when accessed. The file must contain a sorted unique index;
// OpenIndexed does not scan the entire index to establish this invariant.
func OpenIndexed(r io.ReaderAt, size int64, limits IndexedLimits) (*IndexedReader, error) {
	if limits.MaxEntries <= 0 || limits.MaxEntries > math.MaxInt32 || limits.MaxBlobSize < 0 || limits.MaxBlobSize > math.MaxInt32 || limits.MaxEncodedSize <= 0 || limits.MaxEncodedSize > math.MaxInt32 {
		return nil, fmt.Errorf("%w: invalid indexed limits", ErrTooLarge)
	}
	if r == nil || size < int64(len(indexedHeader)+indexedFooterSize) {
		return nil, ErrInvalidPack
	}
	var header [8]byte
	if err := indexedReadAt(r, header[:], 0); err != nil {
		return nil, err
	}
	if string(header[:]) != indexedHeader {
		return nil, fmt.Errorf("%w: header", ErrInvalidPack)
	}
	var footer [indexedFooterSize]byte
	if err := indexedReadAt(r, footer[:], size-indexedFooterSize); err != nil {
		return nil, err
	}
	if string(footer[:8]) != indexedFooter {
		return nil, fmt.Errorf("%w: footer", ErrInvalidPack)
	}
	offset := binary.LittleEndian.Uint64(footer[8:16])
	count := binary.LittleEndian.Uint64(footer[16:24])
	recordSize := binary.LittleEndian.Uint64(footer[24:32])
	if count == 0 || recordSize != indexedRecordSize || offset < 8 || offset > uint64(size-indexedFooterSize) {
		return nil, ErrInvalidPack
	}
	if count > uint64(limits.MaxEntries) {
		return nil, fmt.Errorf("%w: entry count", ErrTooLarge)
	}
	if count > uint64((size-indexedFooterSize)/indexedRecordSize) || count*indexedRecordSize != uint64(size-indexedFooterSize)-offset {
		return nil, ErrInvalidPack
	}
	return &IndexedReader{r: r, indexOffset: int64(offset), count: int64(count), limits: limits}, nil
}

// Len returns the number of distinct content versions in the pack.
func (r *IndexedReader) Len() int64 { return r.count }

// DigestAt inspects one index record, in SHA-256 hash then size order.
func (r *IndexedReader) DigestAt(index int64) (Digest, error) {
	info, err := r.EntryAt(index)
	if err != nil {
		return Digest{}, err
	}
	return info.Digest, nil
}

// EntryAt returns metadata for a sorted index position without reading any
// payload. The referenced base record, if present, is also validated. Caller
// byte limits do not apply to metadata-only inspection.
func (r *IndexedReader) EntryAt(index int64) (VersionInfo, error) {
	record, err := r.record(index)
	if err != nil {
		return VersionInfo{}, err
	}
	return r.versionInfo(record)
}

// Lookup finds an independently supplied digest using on-disk binary search.
// It reads only index metadata, not the target or base payloads. Caller byte
// limits do not apply to metadata-only inspection.
func (r *IndexedReader) Lookup(expected Digest) (VersionInfo, error) {
	record, err := r.find(expected)
	if err != nil {
		return VersionInfo{}, err
	}
	return r.versionInfo(record)
}

func (r *IndexedReader) versionInfo(record indexedRecord) (VersionInfo, error) {
	info := VersionInfo{Digest: record.digest, Offset: int64(record.offset), EncodedSize: int64(record.length), Codec: IndexedCodec(record.codec)}
	if record.codec == indexedCodecDelta {
		anchor, err := r.record(int64(record.base))
		if err != nil {
			return VersionInfo{}, err
		}
		if anchor.codec == indexedCodecDelta {
			return VersionInfo{}, fmt.Errorf("%w: delta chain", ErrInvalidPack)
		}
		base := anchor.digest
		info.BaseDigest = &base
	}
	return info, nil
}

// ReadDelta reads only the delta frame and the metadata needed to name its
// independent base; it never reads the base payload. Unlike ReadVersion, it
// cannot authenticate the target contents without the base. A receiving caller
// must use DecodeDelta with an independently trusted expected target digest.
// Byte limits apply only to the target; its base may exceed those limits since
// no base payload is read or decoded. Independent versions return ErrNotDelta.
// Every error returns a nil packet.
func (r *IndexedReader) ReadDelta(expected Digest) (*Delta, error) {
	record, err := r.find(expected)
	if err != nil {
		return nil, err
	}
	if err := r.checkLimits(record); err != nil {
		return nil, err
	}
	info, err := r.versionInfo(record)
	if err != nil {
		return nil, err
	}
	if info.BaseDigest == nil {
		return nil, ErrNotDelta
	}
	payload, err := r.payload(record)
	if err != nil {
		return nil, err
	}
	if len(payload) < 4 || binary.LittleEndian.Uint32(payload[:4]) != 0xfd2fb528 {
		return nil, fmt.Errorf("%w: missing Zstandard frame", ErrInvalidPack)
	}
	return &Delta{BaseDigest: *info.BaseDigest, TargetDigest: expected, Zstd: payload, DigestFunction: DigestFunctionSHA256}, nil
}

func indexedReadAt(r io.ReaderAt, data []byte, offset int64) error {
	n, err := r.ReadAt(data, offset)
	if n != len(data) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("%w: read at %d: %w", ErrInvalidPack, offset, err)
	}
	// ReaderAt may return io.EOF alongside a complete read ending at EOF.
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: read at %d: %w", ErrInvalidPack, offset, err)
	}
	return nil
}

func (r *IndexedReader) record(index int64) (indexedRecord, error) {
	var result indexedRecord
	if index < 0 || index >= r.count {
		return result, ErrVersionNotFound
	}
	var data [indexedRecordSize]byte
	if err := indexedReadAt(r.r, data[:], r.indexOffset+index*indexedRecordSize); err != nil {
		return result, err
	}
	copy(result.hash[:], data[:32])
	size := binary.LittleEndian.Uint64(data[32:40])
	result.offset = binary.LittleEndian.Uint64(data[40:48])
	result.length = binary.LittleEndian.Uint64(data[48:56])
	result.base = binary.LittleEndian.Uint64(data[56:64])
	result.codec = binary.LittleEndian.Uint64(data[64:72])
	if size > math.MaxInt32 || result.length > math.MaxInt32 {
		return result, fmt.Errorf("%w: indexed format size bounds", ErrTooLarge)
	}
	if result.offset < 8 || result.offset > uint64(r.indexOffset) || result.length > uint64(r.indexOffset)-result.offset {
		return result, ErrInvalidPack
	}
	switch result.codec {
	case indexedCodecRaw:
		if result.length != size || result.base != math.MaxUint64 {
			return result, ErrInvalidPack
		}
	case indexedCodecZstd:
		if result.length == 0 || result.base != math.MaxUint64 {
			return result, ErrInvalidPack
		}
	case indexedCodecDelta:
		if result.length == 0 || result.base >= uint64(r.count) || result.base == uint64(index) {
			return result, ErrInvalidPack
		}
	default:
		return result, ErrInvalidPack
	}
	result.digest = Digest{Hash: hex.EncodeToString(result.hash[:]), SizeBytes: int64(size)}
	return result, nil
}

func compareIndexedDigest(a, b Digest) int {
	if comparison := strings.Compare(a.Hash, b.Hash); comparison != 0 {
		return comparison
	}
	if a.SizeBytes < b.SizeBytes {
		return -1
	}
	if a.SizeBytes > b.SizeBytes {
		return 1
	}
	return 0
}

// ReadVersion locates an independently trusted content digest with on-disk
// binary search, then reads only its payload and, for a delta, one independent
// base payload. Both reconstructed contents are verified by SHA-256 and size.
// All errors return nil output. Memory use is bounded by the selected payloads,
// their uncompressed contents, and an 8 MiB Zstandard decoder window.
func (r *IndexedReader) ReadVersion(expected Digest) ([]byte, error) {
	record, err := r.find(expected)
	if err != nil {
		return nil, err
	}
	return r.decode(record)
}

func (r *IndexedReader) find(expected Digest) (indexedRecord, error) {
	if err := validateDigest(expected); err != nil {
		return indexedRecord{}, err
	}
	low, high := int64(0), r.count
	for low < high {
		mid := low + (high-low)/2
		record, err := r.record(mid)
		if err != nil {
			return indexedRecord{}, err
		}
		comparison := compareIndexedDigest(record.digest, expected)
		if comparison < 0 {
			low = mid + 1
			continue
		}
		if comparison > 0 {
			high = mid
			continue
		}
		return record, nil
	}
	return indexedRecord{}, ErrVersionNotFound
}

func (r *IndexedReader) checkLimits(record indexedRecord) error {
	if record.digest.SizeBytes > r.limits.MaxBlobSize || record.length > uint64(r.limits.MaxEncodedSize) {
		return fmt.Errorf("%w: indexed version limits", ErrTooLarge)
	}
	return nil
}

func (r *IndexedReader) payload(record indexedRecord) ([]byte, error) {
	if err := r.checkLimits(record); err != nil {
		return nil, err
	}
	data := make([]byte, int(record.length))
	if len(data) > 0 {
		if err := indexedReadAt(r.r, data, int64(record.offset)); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (r *IndexedReader) decode(record indexedRecord) ([]byte, error) {
	if err := r.checkLimits(record); err != nil {
		return nil, err
	}
	var base []byte
	var anchor indexedRecord
	if record.codec == indexedCodecDelta {
		var err error
		anchor, err = r.record(int64(record.base))
		if err != nil {
			return nil, err
		}
		if anchor.codec == indexedCodecDelta {
			return nil, fmt.Errorf("%w: delta chain", ErrInvalidPack)
		}
		base, err = r.decode(anchor)
		if err != nil {
			return nil, err
		}
	}
	payload, err := r.payload(record)
	if err != nil {
		return nil, err
	}
	if record.codec == indexedCodecRaw {
		if SHA256Digest(payload) != record.digest {
			return nil, ErrTargetMismatch
		}
		return payload, nil
	}
	if record.codec == indexedCodecDelta {
		return DecodeDelta(base, &Delta{BaseDigest: anchor.digest, TargetDigest: record.digest, Zstd: payload, DigestFunction: DigestFunctionSHA256}, record.digest, r.limits.MaxBlobSize)
	}
	if len(payload) < 4 || binary.LittleEndian.Uint32(payload[:4]) != 0xfd2fb528 {
		return nil, fmt.Errorf("%w: missing Zstandard frame", ErrInvalidPack)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxWindow(8<<20), zstd.WithDecoderMaxMemory(uint64(max(record.digest.SizeBytes, 8<<20))), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPack, err)
	}
	defer decoder.Close()
	output, err := decoder.DecodeAll(payload, make([]byte, 0, int(record.digest.SizeBytes)))
	if err != nil {
		if errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
			return nil, fmt.Errorf("%w: %v", ErrTooLarge, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidPack, err)
	}
	if SHA256Digest(output) != record.digest {
		return nil, ErrTargetMismatch
	}
	return output, nil
}
