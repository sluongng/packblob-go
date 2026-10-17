package packblob_test

import (
	"bytes"
	"fmt"

	"github.com/sluongng/packblob-go"
)

func ExampleIndexedReader() {
	base := []byte("first revision\n")
	target := []byte("second revision\n")
	// The caller knows which version it wants, independently of file metadata.
	expectedTarget := packblob.SHA256Digest(target)
	// Use an *os.File in production; this buffer keeps the example self-contained.
	var file bytes.Buffer
	writer, err := packblob.NewIndexedWriter(&file, 0)
	if err != nil {
		panic(err)
	}
	if _, err := writer.AddBase(base); err != nil {
		panic(err)
	}
	if _, err := writer.AddDelta(base, target); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	reader, err := packblob.OpenIndexed(bytes.NewReader(file.Bytes()), int64(file.Len()), packblob.IndexedLimits{
		MaxEntries:     100,
		MaxBlobSize:    1 << 20,
		MaxEncodedSize: 1 << 20,
	})
	if err != nil {
		panic(err)
	}
	version, err := reader.ReadVersion(expectedTarget)
	if err != nil {
		panic(err)
	}
	fmt.Println(reader.Len())
	fmt.Print(string(version))
	// Output:
	// 2
	// second revision
}

func ExampleBuilder() {
	var builder packblob.Builder
	if err := builder.AddBase("content.base", []byte("first revision\n")); err != nil {
		panic(err)
	}
	if err := builder.AddDelta("content.base", "content.next", []byte("second revision\n")); err != nil {
		panic(err)
	}
	blob, err := builder.Build("packed/")
	if err != nil {
		panic(err)
	}
	fmt.Println(blob.LinkKeys)
	// Output: [content.base content.next]
}

func ExampleWithEncoding() {
	builder, err := packblob.NewBuilder(3, packblob.WithEncoding(packblob.Thrift))
	if err != nil {
		panic(err)
	}
	if err := builder.AddBase("content.base", []byte("first revision\n")); err != nil {
		panic(err)
	}
	blob, err := builder.Build("packed/")
	if err != nil {
		panic(err)
	}
	fmt.Println(blob.Encoding == packblob.Thrift)
	fmt.Println(blob.LinkKeys)
	// Output:
	// true
	// [content.base]
}

func ExampleEncodeDelta() {
	// Both hosts already have base. The receiver obtains expectedTarget from
	// its trusted request context, independently of the incoming packet.
	base := []byte("first revision\n")
	target := []byte("second revision\n")
	expectedTarget := packblob.SHA256Digest(target)
	delta, err := packblob.EncodeDelta(base, target, 0)
	if err != nil {
		panic(err)
	}
	wire, err := delta.MarshalBinary() // Only this packet crosses the wire.
	if err != nil {
		panic(err)
	}
	received, err := packblob.UnmarshalDelta(wire, 1<<20)
	if err != nil {
		panic(err)
	}
	decoded, err := packblob.DecodeDelta(base, received, expectedTarget, 1<<20)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(decoded))
	// Output: second revision
}
