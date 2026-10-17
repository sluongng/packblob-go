package packblob_test

import (
	"fmt"

	"github.com/sluongng/packblob-go"
)

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
