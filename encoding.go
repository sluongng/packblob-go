package packblob

import (
	"errors"
	"fmt"
	"math"

	"github.com/sluongng/packblob-go/protobuf"
	"github.com/sluongng/packblob-go/thrift"
	"google.golang.org/protobuf/proto"
)

// Encoding selects the storage-envelope wire format. Enum values are not the
// on-disk header values; see the constants for their framing and compatibility.
type Encoding uint8

const (
	// Protobuf is the default. It uses a four-byte big-endian header of 1
	// followed by a protobuf StorageEnvelope, with the .pack.pb storage suffix.
	// This is a library extension, not a format supported by Mononoke's reader.
	Protobuf Encoding = iota
	// Thrift uses Mononoke's header of 0 and Thrift Compact Protocol, with the
	// .pack storage suffix. Select it when writing for existing Mononoke readers.
	Thrift
)

type options struct {
	encoding Encoding
}

// Option configures a builder or an independent-envelope encoding operation.
type Option func(*options) error

// WithEncoding selects the envelope encoding. Without this option, builders,
// EncodeSingle, and EncodeRaw use Protobuf. Unsupported values return
// ErrInvalidEncoding when the option is applied.
func WithEncoding(encoding Encoding) Option {
	return func(configuration *options) error {
		if encoding != Protobuf && encoding != Thrift {
			return fmt.Errorf("%w: %d", ErrInvalidEncoding, encoding)
		}
		configuration.encoding = encoding
		return nil
	}
}

func resolveOptions(opts []Option) (options, error) {
	configuration := options{encoding: Protobuf}
	for _, option := range opts {
		if option == nil {
			return options{}, errors.New("packblob: nil option")
		}
		if err := option(&configuration); err != nil {
			return options{}, err
		}
	}
	return configuration, nil
}

func (encoding Encoding) suffix() string {
	if encoding == Thrift {
		return ThriftEnvelopeSuffix
	}
	return EnvelopeSuffix
}

func marshalSingle(encoding Encoding, data []byte, compressed bool) ([]byte, error) {
	if encoding == Thrift {
		encoded, err := thrift.MarshalSingle(data, compressed)
		return encoded, thriftError(err)
	}
	single := new(protobuf.SingleValue)
	if compressed {
		single.SetZstd(data)
	} else {
		single.SetRaw(data)
	}
	storage := protobuf.StorageFormat_builder{Single: single}.Build()
	return marshalProtobuf(protobuf.StorageEnvelope_builder{Storage: storage}.Build())
}

func marshalPack(encoding Encoding, key string, entries []entry) ([]byte, error) {
	if encoding == Thrift {
		values := make([]thrift.Entry, len(entries))
		for i, entry := range entries {
			values[i] = thrift.Entry{Key: entry.key, Zstd: entry.zstd, Dictionary: entry.dictionary}
		}
		encoded, err := thrift.MarshalPack(key, values)
		return encoded, thriftError(err)
	}
	values := make([]*protobuf.PackedEntry, len(entries))
	for i, entry := range entries {
		value := new(protobuf.PackedValue)
		if entry.dictionary != nil {
			value.SetZstdFromDict(protobuf.ZstdFromDictValue_builder{
				DictKey: entry.dictionary,
				Zstd:    entry.zstd,
			}.Build())
		} else {
			value.SetSingle(protobuf.SingleValue_builder{Zstd: entry.zstd}.Build())
		}
		values[i] = protobuf.PackedEntry_builder{Key: proto.String(entry.key), Data: value}.Build()
	}
	pack := protobuf.PackedFormat_builder{Key: proto.String(key), Entries: values}.Build()
	storage := protobuf.StorageFormat_builder{Packed: pack}.Build()
	return marshalProtobuf(protobuf.StorageEnvelope_builder{Storage: storage}.Build())
}

func marshalProtobuf(envelope *protobuf.StorageEnvelope) ([]byte, error) {
	if proto.Size(envelope) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("packblob: marshal protobuf envelope: %w", err)
	}
	return append([]byte{0, 0, 0, 1}, body...), nil
}

func thriftError(err error) error {
	if errors.Is(err, thrift.ErrTooLarge) {
		return ErrTooLarge
	}
	return err
}
