package packblob

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
	"github.com/sluongng/packblob-go/thrift"
	"golang.org/x/crypto/blake2b"
)

// EnvelopeSuffix is appended to logical blob keys by Mononoke's packblob store.
const EnvelopeSuffix = ".pack"

var (
	ErrEmptyPack         = errors.New("packblob: pack has no entries")
	ErrDuplicateKey      = errors.New("packblob: duplicate key")
	ErrMissingDictionary = errors.New("packblob: dictionary must be an earlier entry")
	ErrInvalidDictionary = errors.New("packblob: formatted Zstandard dictionaries are not supported")
	ErrInvalidKey        = errors.New("packblob: invalid key")
	ErrInvalidLevel      = errors.New("packblob: unsupported compression level")
	ErrTooLarge          = thrift.ErrTooLarge
)

// Mononoke rejects repository and ephemeral repository prefixes inside a pack.
// Keep these separate from the caller's pack storage prefix.
var repositoryPrefix = regexp.MustCompile(`^(repo[\p{Nd}]{4,}\.|eph[\p{Nd}]+\.repo[\p{Nd}]{4,}\.)`)

// Blob is a serialized pack ready for storage. Its slices belong to the caller.
type Blob struct {
	// Key is prefix + pack identity + EnvelopeSuffix, also embedded in Data.
	Key string
	// LinkKeys contains sorted, unprefixed logical keys without EnvelopeSuffix.
	// Underlying links use repositoryPrefix + logicalKey + EnvelopeSuffix.
	LinkKeys []string
	// Data is the complete at-rest envelope, including its four-byte header.
	Data []byte
}

// Builder creates an ordered pack. It retains copies of the raw data for all
// entries so any earlier entry can serve as a dictionary. It also retains each
// compressed entry. Use bounded packs to limit memory use.
//
// The zero value is ready to use with default compression. A Builder must not be
// copied after use and is not safe for concurrent calls. Separate builders may
// be used concurrently. Build does not consume or reset the builder.
type Builder struct {
	level        int
	entries      []thrift.Entry
	dictionaries map[string][]byte
}

// NewBuilder creates an empty builder. zstdLevel is a Zstandard-style level:
// zero selects the default (3), negative levels favor speed, and the maximum
// is 22. Values below -131072 are rejected. The pure-Go encoder maps levels to
// its nearest preset; output and compression ratios can differ from libzstd.
func NewBuilder(zstdLevel int) (*Builder, error) {
	if err := validateLevel(zstdLevel); err != nil {
		return nil, err
	}
	return &Builder{level: zstdLevel}, nil
}

// AddBase adds an independent Zstandard-compressed entry. The first entry must
// be a base. Further bases are allowed, though dictionary compression usually
// saves more space for related values. Even empty values use a Zstandard frame.
// key must be UTF-8 and must not contain a Mononoke repository prefix.
func (b *Builder) AddBase(key string, data []byte) error {
	return b.add("", key, data, false)
}

// AddDelta adds an entry compressed with an earlier entry's raw contents as
// its dictionary. Dictionary chains are allowed; cycles and forward references
// are rejected. Dictionaries of at least eight bytes beginning with Zstandard's
// formatted-dictionary magic are rejected: Mononoke would interpret them as
// trained dictionaries, not raw blob contents. An error leaves the builder
// unchanged.
func (b *Builder) AddDelta(dictKey, key string, data []byte) error {
	return b.add(dictKey, key, data, true)
}

func (b *Builder) add(dictKey, key string, data []byte, delta bool) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if _, exists := b.dictionaries[key]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateKey, key)
	}
	if len(b.entries) >= math.MaxInt32 || len(data) > math.MaxInt32 {
		return ErrTooLarge
	}
	var dictionary []byte
	if delta {
		var exists bool
		dictionary, exists = b.dictionaries[dictKey]
		if !exists {
			return fmt.Errorf("%w: %q", ErrMissingDictionary, dictKey)
		}
		// libzstd auto-detects formatted dictionaries from this magic when the
		// dictionary has at least eight bytes. Its reader cannot force raw mode.
		if len(dictionary) >= 8 && binary.LittleEndian.Uint32(dictionary[:4]) == 0xec30a437 {
			return fmt.Errorf("%w: %q starts with dictionary magic", ErrInvalidDictionary, dictKey)
		}
	}
	compressed, err := compress(data, b.level, dictionary, delta)
	if err != nil {
		return err
	}
	if b.dictionaries == nil {
		b.dictionaries = make(map[string][]byte)
	}
	b.dictionaries[key] = bytes.Clone(data)
	e := thrift.Entry{Key: key, Zstd: compressed}
	if delta {
		e.Dictionary = &dictKey
	}
	b.entries = append(b.entries, e)
	return nil
}

// Build serializes the pack without consuming the builder. prefix is an
// arbitrary UTF-8 storage-key prefix, not a repository prefix for the entries.
// Each call returns independent slices. The pack key identifies the sorted set
// of logical keys, not compressed bytes or entry order.
func (b *Builder) Build(prefix string) (*Blob, error) {
	if len(b.entries) == 0 {
		return nil, ErrEmptyPack
	}
	if !utf8.ValidString(prefix) {
		return nil, fmt.Errorf("%w: pack prefix is not UTF-8", ErrInvalidKey)
	}
	if len(prefix) > math.MaxInt32-64-len(EnvelopeSuffix) {
		return nil, ErrTooLarge
	}
	keys := make([]string, len(b.entries))
	for i, e := range b.entries {
		keys[i] = e.Key
	}
	sort.Strings(keys)
	hash, _ := blake2b.New256([]byte("pack")) // Fixed, valid BLAKE2b key.
	var size [8]byte
	for _, key := range keys {
		binary.LittleEndian.PutUint64(size[:], uint64(len(key)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(key))
	}
	key := prefix + hex.EncodeToString(hash.Sum(nil)) + EnvelopeSuffix
	data, err := thrift.MarshalPack(key, b.entries)
	if err != nil {
		return nil, err
	}
	return &Blob{Key: key, LinkKeys: keys, Data: data}, nil
}

// EncodeSingle wraps data in an independent storage envelope. It uses Zstandard
// only if the compressed payload is strictly smaller than data. zstdLevel has
// the same meaning as in NewBuilder. The returned slice is independent of data.
func EncodeSingle(data []byte, zstdLevel int) ([]byte, error) {
	compressed, err := compress(data, zstdLevel, nil, false)
	if err != nil {
		return nil, err
	}
	if len(compressed) < len(data) {
		return thrift.MarshalSingle(compressed, true)
	}
	return EncodeRaw(data)
}

// EncodeRaw wraps data in an uncompressed independent storage envelope.
// The returned slice is independent of data.
func EncodeRaw(data []byte) ([]byte, error) {
	if len(data) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	return thrift.MarshalSingle(data, false)
}

func validateKey(key string) error {
	if !utf8.ValidString(key) || repositoryPrefix.MatchString(key) {
		return fmt.Errorf("%w: keys must be UTF-8 without repository prefixes: %q", ErrInvalidKey, key)
	}
	if len(key) > math.MaxInt32 {
		return ErrTooLarge
	}
	return nil
}

func validateLevel(level int) error {
	if level < -131072 || level > 22 {
		return fmt.Errorf("%w: %d (want -131072 through 22)", ErrInvalidLevel, level)
	}
	return nil
}

func compress(data []byte, level int, dictionary []byte, useDictionary bool) ([]byte, error) {
	if err := validateLevel(level); err != nil {
		return nil, err
	}
	if len(data) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	if level == 0 {
		level = 3
	}
	opts := []zstd.EOption{
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithEncoderConcurrency(1),
		zstd.WithZeroFrames(true),
	}
	if useDictionary {
		// ID zero matches libzstd's raw-content dictionaries. A synthetic nonzero
		// ID would make Mononoke reject the frame with a dictionary mismatch.
		opts = append(opts, zstd.WithEncoderDictRaw(0, dictionary))
	}
	encoder, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("packblob: create Zstandard encoder: %w", err)
	}
	defer encoder.Close()
	compressed := encoder.EncodeAll(data, nil)
	if len(compressed) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	return compressed, nil
}
