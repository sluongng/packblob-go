package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/sluongng/packblob-go/examples/transfer"
)

func main() {
	server := flag.String("server", "http://127.0.0.1:8090", "server URL")
	basePath := flag.String("base", "", "optional base file for the first upload")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] FILE [FILE...]\nEach successful file becomes the candidate base for the next.\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	var base []byte
	if *basePath != "" {
		var err error
		base, err = readFile(*basePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	client := &transfer.Client{URL: *server}
	for _, path := range flag.Args() {
		target, err := readFile(path)
		if err != nil {
			log.Fatal(err)
		}
		result, err := client.Upload(context.Background(), base, target)
		if err != nil {
			log.Fatalf("%s: %v", path, err)
		}
		fmt.Printf("%s: mode=%s sha256=%s size=%d upload_bytes=%d verified=true\n",
			path, result.Mode, result.Digest.Hash, result.Digest.SizeBytes, result.BytesSent)
		base = target
	}
}

func readFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, transfer.MaxFileSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > transfer.MaxFileSize {
		return nil, fmt.Errorf("%s: file exceeds %d bytes", path, transfer.MaxFileSize)
	}
	return data, nil
}
