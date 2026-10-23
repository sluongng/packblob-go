package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/sluongng/packblob-go/examples/transfer"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8090", "HTTP listen address")
	flag.Parse()
	server := &http.Server{
		Addr:              *listen,
		Handler:           transfer.NewServer(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("listening on http://%s (in-memory example)", *listen)
	log.Fatal(server.ListenAndServe())
}
