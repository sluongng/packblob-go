package packblob

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/sluongng/packblob-go/protobuf"
	"google.golang.org/protobuf/proto"
)

// DigestFunctionSHA256 is the digest function supported by standalone deltas.
const DigestFunctionSHA256 = "sha256"

var (
	ErrInvalidDelta              = errors.New("packblob: invalid delta metadata")
	ErrUnsupportedDigestFunction = errors.New("packblob: unsupported digest function")
	ErrBaseMismatch              = errors.New("packblob: base does not match delta digest")
	ErrTargetMismatch            = errors.New("packblob: target does not match expected digest")
	ErrCorruptDelta              = errors.New("packblob: corrupt delta payload")
)

// Digest identifies uncompressed content by its lowercase hexadecimal SHA-256
// hash and byte length. It is independent of the pack storage-key scheme.
type Digest struct {
	Hash      string
	SizeBytes int64
}

// SHA256Digest returns the content identity of data.
func SHA256Digest(data []byte) Digest {
	hash := sha256.Sum256(data)
	return Digest{Hash: hex.EncodeToString(hash[:]), SizeBytes: int64(len(data))}
}

// Delta contains a Zstandard frame compressed with an external base's raw
// contents as its dictionary. It does not contain the base. Callers must
// negotiate base availability and independently obtain the expected target
// digest; neither negotiation nor storage is implemented by this codec.
// Zstd belongs to the caller. Do not mutate a Delta during a codec operation.
type Delta struct {
	BaseDigest     Digest
	TargetDigest   Digest
	Zstd           []byte
	DigestFunction string
}

// EncodeDelta compresses target using base without including base in the
// returned packet. zstdLevel has the same meaning as in NewBuilder. Empty
// bases and targets are supported. Base is always interpreted as raw bytes,
// including contents beginning with trained-dictionary magic. Returned data
// does not alias the inputs.
func EncodeDelta(base, target []byte, zstdLevel int) (*Delta, error) {
	if len(base) > math.MaxInt32 || len(target) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	if err := validateLevel(zstdLevel); err != nil {
		return nil, err
	}
	compressed, err := compress(target, zstdLevel, base, true)
	if err != nil {
		return nil, err
	}
	return &Delta{
		BaseDigest: SHA256Digest(base), TargetDigest: SHA256Digest(target),
		Zstd: compressed, DigestFunction: DigestFunctionSHA256,
	}, nil
}

// DecodeDelta reconstructs a target using a locally available base. It verifies
// both content identities and requires an independently supplied expectedTarget
// so an untrusted packet cannot choose which target is accepted. maxOutputSize
// must be between zero and math.MaxInt32; zero permits only an empty target.
// The decoder bounds its window to 8 MiB and its output to the expected size.
// Every error returns nil output, including errors after partial decoding.
func DecodeDelta(base []byte, delta *Delta, expectedTarget Digest, maxOutputSize int64) ([]byte, error) {
	if maxOutputSize < 0 || maxOutputSize > math.MaxInt32 {
		return nil, fmt.Errorf("%w: invalid output limit", ErrTooLarge)
	}
	if err := validateDelta(delta); err != nil {
		return nil, err
	}
	if err := validateDigest(expectedTarget); err != nil {
		return nil, fmt.Errorf("expected target: %w", err)
	}
	if expectedTarget != delta.TargetDigest {
		return nil, ErrTargetMismatch
	}
	if expectedTarget.SizeBytes > maxOutputSize {
		return nil, fmt.Errorf("%w: target exceeds output limit", ErrTooLarge)
	}
	if len(base) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	if SHA256Digest(base) != delta.BaseDigest {
		return nil, ErrBaseMismatch
	}
	// A packet must begin with a real Zstandard frame, not just skippable
	// metadata (which DecodeAll would otherwise accept as an empty target).
	if len(delta.Zstd) < 4 || binary.LittleEndian.Uint32(delta.Zstd[:4]) != 0xfd2fb528 {
		return nil, fmt.Errorf("%w: missing Zstandard frame magic", ErrCorruptDelta)
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderDictRaw(0, base),
		zstd.WithDecoderMaxWindow(8<<20),
		// The decoder also uses this bound for frame windows. Small targets
		// can still carry the encoder's 8 MiB window; the output cap below
		// independently enforces their exact reconstructed size.
		zstd.WithDecoderMaxMemory(uint64(max(expectedTarget.SizeBytes, 8<<20))),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: create decoder: %v", ErrCorruptDelta, err)
	}
	defer decoder.Close()
	output, err := decoder.DecodeAll(delta.Zstd, make([]byte, 0, int(expectedTarget.SizeBytes)))
	if err != nil {
		if errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
			return nil, fmt.Errorf("%w: %v", ErrTooLarge, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrCorruptDelta, err)
	}
	if SHA256Digest(output) != expectedTarget {
		return nil, ErrTargetMismatch
	}
	return output, nil
}

// MarshalBinary returns a bare protobuf StandaloneDelta message, without a
// storage-envelope header. It validates metadata but does not decode the frame.
func (delta *Delta) MarshalBinary() ([]byte, error) {
	if err := validateDelta(delta); err != nil {
		return nil, err
	}
	message := protobuf.StandaloneDelta_builder{
		BaseDigest:     digestMessage(delta.BaseDigest),
		TargetDigest:   digestMessage(delta.TargetDigest),
		Zstd:           delta.Zstd,
		DigestFunction: new(delta.DigestFunction),
	}.Build()
	if proto.Size(message) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDelta, err)
	}
	return data, nil
}

// UnmarshalDelta parses a bare protobuf StandaloneDelta. maxEncodedSize must
// be between zero and math.MaxInt32 and is checked before parsing. The returned
// packet does not retain data. Missing fields and unknown fields are rejected;
// metadata is validated, but frame integrity is checked only by DecodeDelta.
func UnmarshalDelta(data []byte, maxEncodedSize int64) (*Delta, error) {
	if maxEncodedSize < 0 || maxEncodedSize > math.MaxInt32 || int64(len(data)) > maxEncodedSize {
		return nil, fmt.Errorf("%w: invalid or exceeded encoded-size limit", ErrTooLarge)
	}
	message := new(protobuf.StandaloneDelta)
	if err := (proto.UnmarshalOptions{RecursionLimit: 16}).Unmarshal(data, message); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDelta, err)
	}
	base, target := message.GetBaseDigest(), message.GetTargetDigest()
	if !message.HasBaseDigest() || !message.HasTargetDigest() || !message.HasZstd() || !message.HasDigestFunction() ||
		!base.HasHash() || !base.HasSizeBytes() || !target.HasHash() || !target.HasSizeBytes() ||
		len(message.ProtoReflect().GetUnknown()) != 0 || len(base.ProtoReflect().GetUnknown()) != 0 || len(target.ProtoReflect().GetUnknown()) != 0 {
		return nil, fmt.Errorf("%w: missing or unknown fields", ErrInvalidDelta)
	}
	delta := &Delta{
		BaseDigest:   Digest{Hash: base.GetHash(), SizeBytes: base.GetSizeBytes()},
		TargetDigest: Digest{Hash: target.GetHash(), SizeBytes: target.GetSizeBytes()},
		Zstd:         message.GetZstd(), DigestFunction: message.GetDigestFunction(),
	}
	if err := validateDelta(delta); err != nil {
		return nil, err
	}
	return delta, nil
}

func digestMessage(digest Digest) *protobuf.BlobDigest {
	return protobuf.BlobDigest_builder{Hash: new(digest.Hash), SizeBytes: new(digest.SizeBytes)}.Build()
}

func validateDelta(delta *Delta) error {
	if delta == nil {
		return ErrInvalidDelta
	}
	if delta.DigestFunction == "" {
		return fmt.Errorf("%w: missing digest function", ErrInvalidDelta)
	}
	if delta.DigestFunction != DigestFunctionSHA256 {
		return ErrUnsupportedDigestFunction
	}
	if err := validateDigest(delta.BaseDigest); err != nil {
		return fmt.Errorf("base: %w", err)
	}
	if err := validateDigest(delta.TargetDigest); err != nil {
		return fmt.Errorf("target: %w", err)
	}
	if len(delta.Zstd) == 0 {
		return fmt.Errorf("%w: missing Zstandard frame", ErrInvalidDelta)
	}
	if len(delta.Zstd) > math.MaxInt32 {
		return ErrTooLarge
	}
	return nil
}

func validateDigest(digest Digest) error {
	if digest.SizeBytes < 0 {
		return fmt.Errorf("%w: negative digest size", ErrInvalidDelta)
	}
	if digest.SizeBytes > math.MaxInt32 {
		return fmt.Errorf("%w: digest size", ErrTooLarge)
	}
	if len(digest.Hash) != sha256.Size*2 {
		return fmt.Errorf("%w: SHA-256 hash must have 64 lowercase hexadecimal characters", ErrInvalidDelta)
	}
	for _, char := range digest.Hash {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return fmt.Errorf("%w: SHA-256 hash must be lowercase hexadecimal", ErrInvalidDelta)
		}
	}
	return nil
}
