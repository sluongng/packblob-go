// Package transfer demonstrates negotiated full-file or delta uploads over HTTP.
// It is an in-memory example, not a production blob service.
package transfer

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/sluongng/packblob-go"
	transferpb "github.com/sluongng/packblob-go/examples/transfer/protobuf"
	"github.com/sluongng/packblob-go/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	MaxFileSize    = 4 << 20
	maxMessageSize = MaxFileSize + (64 << 10)
)

// Server stores verified full contents by digest. Blobs live until process exit;
// this example has no eviction, persistence, or total-storage quota.
type Server struct {
	mu    sync.RWMutex
	blobs map[packblob.Digest][]byte
}

func NewServer() *Server {
	return &Server{blobs: make(map[packblob.Digest][]byte)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	switch r.URL.Path {
	case "/findmissing":
		s.findMissing(w, r)
	case "/upload":
		s.upload(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) findMissing(w http.ResponseWriter, r *http.Request) {
	req := new(transferpb.FindMissingRequest)
	if !readMessage(w, r, req) {
		return
	}
	var missing []*protobuf.BlobDigest
	for _, message := range req.GetDigests() {
		digest, err := parseDigest(message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.RLock()
		_, found := s.blobs[digest]
		s.mu.RUnlock()
		if !found {
			missing = append(missing, message)
		}
	}
	writeMessage(w, transferpb.FindMissingResponse_builder{Missing: missing}.Build())
}

func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	req := new(transferpb.UploadRequest)
	if !readMessage(w, r, req) {
		return
	}
	expected, err := parseDigest(req.GetTargetDigest())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var contents []byte
	mode := "full"
	switch {
	case req.HasFile():
		contents = req.GetFile()
	case req.HasDelta():
		mode = "delta"
		wire, err := proto.Marshal(req.GetDelta())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		delta, err := packblob.UnmarshalDelta(wire, maxMessageSize)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.RLock()
		base, found := s.blobs[delta.BaseDigest]
		s.mu.RUnlock()
		if !found {
			// FindMissing is a snapshot, not a promise to retain a base.
			http.Error(w, "base missing; retry with full file", http.StatusConflict)
			return
		}
		contents, err = packblob.DecodeDelta(base, delta, expected, MaxFileSize)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "file or delta required", http.StatusBadRequest)
		return
	}
	// This also checks full-file uploads. Never publish unverified contents.
	if packblob.SHA256Digest(contents) != expected {
		http.Error(w, "target checksum or size mismatch", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.blobs[expected] = contents
	s.mu.Unlock()
	log.Printf("verified sha256=%s size=%d mode=%s", expected.Hash, expected.SizeBytes, mode)
	writeMessage(w, transferpb.UploadResponse_builder{TargetDigest: digestMessage(expected)}.Build())
}

func parseDigest(message *protobuf.BlobDigest) (packblob.Digest, error) {
	if message == nil || !message.HasHash() || !message.HasSizeBytes() {
		return packblob.Digest{}, fmt.Errorf("SHA-256 hash and size required")
	}
	digest := packblob.Digest{Hash: message.GetHash(), SizeBytes: message.GetSizeBytes()}
	hash, err := hex.DecodeString(digest.Hash)
	if err != nil || len(hash) != 32 || digest.Hash != strings.ToLower(digest.Hash) ||
		digest.SizeBytes < 0 || digest.SizeBytes > MaxFileSize {
		return packblob.Digest{}, fmt.Errorf("invalid SHA-256 digest or file exceeds %d bytes", MaxFileSize)
	}
	return digest, nil
}

func digestMessage(digest packblob.Digest) *protobuf.BlobDigest {
	return protobuf.BlobDigest_builder{Hash: proto.String(digest.Hash), SizeBytes: proto.Int64(digest.SizeBytes)}.Build()
}

func readMessage(w http.ResponseWriter, r *http.Request, message proto.Message) bool {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMessageSize))
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return false
	}
	if err := (proto.UnmarshalOptions{RecursionLimit: 16}).Unmarshal(data, message); err != nil {
		http.Error(w, "invalid protobuf", http.StatusBadRequest)
		return false
	}
	return true
}

func writeMessage(w http.ResponseWriter, message proto.Message) {
	data, err := proto.Marshal(message)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/protobuf")
	_, _ = w.Write(data)
}
