package transfer_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/sluongng/packblob-go"
	"github.com/sluongng/packblob-go/examples/transfer"
	transferpb "github.com/sluongng/packblob-go/examples/transfer/protobuf"
	"github.com/sluongng/packblob-go/protobuf"
	"google.golang.org/protobuf/proto"
)

func TestNegotiatedUploads(t *testing.T) {
	base, target := revisions(t)
	uploads := make(chan []byte, 2)
	handler := transfer.NewServer()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/upload" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			uploads <- body
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := &transfer.Client{URL: server.URL, HTTPClient: server.Client()}
	ctx := context.Background()
	baseDigest, targetDigest := packblob.SHA256Digest(base), packblob.SHA256Digest(target)
	assertMissing(t, client, []packblob.Digest{baseDigest, targetDigest}, []packblob.Digest{baseDigest, targetDigest})

	first, err := client.Upload(ctx, nil, base)
	if err != nil {
		t.Fatal(err)
	}
	firstWire := <-uploads
	firstRequest := new(transferpb.UploadRequest)
	if err := proto.Unmarshal(firstWire, firstRequest); err != nil {
		t.Fatal(err)
	}
	if first.Mode != "full" || first.Digest != baseDigest || !firstRequest.HasFile() ||
		firstRequest.HasDelta() || !bytes.Equal(firstRequest.GetFile(), base) || first.BytesSent != len(firstWire) {
		t.Fatalf("first upload did not send the complete base: %+v", first)
	}
	assertMissing(t, client, []packblob.Digest{baseDigest, targetDigest}, []packblob.Digest{targetDigest})

	second, err := client.Upload(ctx, base, target)
	if err != nil {
		t.Fatal(err)
	}
	secondWire := <-uploads
	secondRequest := new(transferpb.UploadRequest)
	if err := proto.Unmarshal(secondWire, secondRequest); err != nil {
		t.Fatal(err)
	}
	if second.Mode != "delta" || second.Digest != targetDigest || secondRequest.HasFile() ||
		!secondRequest.HasDelta() || second.BytesSent != len(secondWire) {
		t.Fatalf("second upload did not send only a delta: %+v", second)
	}
	full := transferpb.UploadRequest_builder{TargetDigest: digestMessage(targetDigest)}.Build()
	full.SetFile(target)
	if len(secondWire) >= proto.Size(full) || bytes.Contains(secondWire, base) {
		t.Fatal("delta upload did not avoid transmitting the base")
	}
	packet, err := proto.Marshal(secondRequest.GetDelta())
	if err != nil {
		t.Fatal(err)
	}
	delta, err := packblob.UnmarshalDelta(packet, int64(len(packet)))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := packblob.DecodeDelta(base, delta, targetDigest, transfer.MaxFileSize)
	if err != nil || !bytes.Equal(decoded, target) {
		t.Fatalf("transmitted delta reconstructed different contents: %v", err)
	}
	assertMissing(t, client, []packblob.Digest{baseDigest, targetDigest}, nil)
}

func TestFullFileFallbacks(t *testing.T) {
	base, target := revisions(t)
	for _, tc := range []struct {
		name         string
		base, target []byte
		seedBase     bool
	}{
		{name: "base is missing", base: base, target: target},
		{name: "delta metadata outweighs tiny target", base: base, target: []byte("tiny"), seedBase: true},
		{name: "empty file still selects file payload", target: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(transfer.NewServer())
			defer server.Close()
			client := &transfer.Client{URL: server.URL, HTTPClient: server.Client()}
			if tc.seedBase {
				if _, err := client.Upload(context.Background(), nil, tc.base); err != nil {
					t.Fatal(err)
				}
			}
			result, err := client.Upload(context.Background(), tc.base, tc.target)
			if err != nil || result.Mode != "full" || result.Digest != packblob.SHA256Digest(tc.target) {
				t.Fatalf("Upload = %+v, %v; want full target", result, err)
			}
			assertMissing(t, client, []packblob.Digest{result.Digest}, nil)
		})
	}
}

func TestMissingBaseAfterNegotiationRetriesFullTarget(t *testing.T) {
	base, target := revisions(t)
	uploads := make(chan []byte, 2)
	handler := transfer.NewServer()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/findmissing" {
			// Simulate a base that was available during negotiation but evicted
			// before upload. An empty protobuf response reports no missing base.
			w.Header().Set("Content-Type", "application/protobuf")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		uploads <- body
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := &transfer.Client{URL: server.URL, HTTPClient: server.Client()}
	result, err := client.Upload(context.Background(), base, target)
	if err != nil || result.Mode != "full" {
		t.Fatalf("Upload = %+v, %v; want successful full retry", result, err)
	}
	firstWire, secondWire := <-uploads, <-uploads
	first, second := new(transferpb.UploadRequest), new(transferpb.UploadRequest)
	if err := proto.Unmarshal(firstWire, first); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(secondWire, second); err != nil {
		t.Fatal(err)
	}
	if !first.HasDelta() || !second.HasFile() || !bytes.Equal(second.GetFile(), target) ||
		result.BytesSent != len(firstWire)+len(secondWire) {
		t.Fatal("missing base did not cause exactly a delta attempt followed by the full target")
	}
	// Query the real handler, not the simulated stale negotiation response.
	probe := httptest.NewServer(handler)
	defer probe.Close()
	assertMissing(t, &transfer.Client{URL: probe.URL, HTTPClient: probe.Client()}, []packblob.Digest{result.Digest}, nil)
}

func TestServerRejectsUnverifiedContents(t *testing.T) {
	base, target := revisions(t)
	targetDigest := packblob.SHA256Digest(target)
	delta, err := packblob.EncodeDelta(base, target, 0)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := delta.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	message := new(protobuf.StandaloneDelta)
	if err := proto.Unmarshal(wire, message); err != nil {
		t.Fatal(err)
	}
	wrongTarget := packblob.SHA256Digest([]byte("a different requested target"))
	corrupt := proto.Clone(message).(*protobuf.StandaloneDelta)
	corrupt.SetZstd([]byte("not a Zstandard frame"))
	wrongContents := bytes.Clone(target)
	wrongContents[0] ^= 1
	for _, tc := range []struct {
		name     string
		expected packblob.Digest
		file     []byte
		delta    *protobuf.StandaloneDelta
	}{
		{name: "full file checksum mismatch", expected: targetDigest, file: wrongContents},
		{name: "full file size mismatch", expected: targetDigest, file: append(bytes.Clone(target), '!')},
		{name: "delta cannot replace outer target identity", expected: wrongTarget, delta: message},
		{name: "corrupt delta", expected: targetDigest, delta: corrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(transfer.NewServer())
			defer server.Close()
			client := &transfer.Client{URL: server.URL, HTTPClient: server.Client()}
			if _, err := client.Upload(context.Background(), nil, base); err != nil {
				t.Fatal(err)
			}
			req := transferpb.UploadRequest_builder{TargetDigest: digestMessage(tc.expected)}.Build()
			if tc.delta != nil {
				req.SetDelta(tc.delta)
			} else {
				req.SetFile(tc.file)
			}
			body, err := proto.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			response, err := server.Client().Post(server.URL+"/upload", "application/protobuf", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("invalid upload status = %d; want 400", response.StatusCode)
			}
			assertMissing(t, client, []packblob.Digest{tc.expected}, []packblob.Digest{tc.expected})
		})
	}
}

func TestTransferSizeLimits(t *testing.T) {
	oversized := bytes.Repeat([]byte{'x'}, transfer.MaxFileSize+1)
	client := &transfer.Client{URL: "http://unused.invalid"}
	if _, err := client.Upload(context.Background(), nil, oversized); err == nil {
		t.Fatal("client accepted an oversized file")
	}
	handler := transfer.NewServer()
	req := transferpb.UploadRequest_builder{TargetDigest: digestMessage(packblob.SHA256Digest(oversized))}.Build()
	req.SetFile(oversized)
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(body)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized target status = %d; want 400", w.Code)
	}
	w = httptest.NewRecorder()
	body = bytes.Repeat([]byte{'x'}, transfer.MaxFileSize+(64<<10)+1)
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(body)))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized protobuf body status = %d; want 413", w.Code)
	}
}

func revisions(t *testing.T) ([]byte, []byte) {
	t.Helper()
	base, err := os.ReadFile("testdata/first.txt")
	if err != nil {
		t.Fatal(err)
	}
	target, err := os.ReadFile("testdata/second.txt")
	if err != nil {
		t.Fatal(err)
	}
	return base, target
}

func digestMessage(digest packblob.Digest) *protobuf.BlobDigest {
	return protobuf.BlobDigest_builder{Hash: proto.String(digest.Hash), SizeBytes: proto.Int64(digest.SizeBytes)}.Build()
}

func assertMissing(t *testing.T, client *transfer.Client, requested, want []packblob.Digest) {
	t.Helper()
	got, err := client.FindMissing(context.Background(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || (len(got) != 0 && !reflect.DeepEqual(got, want)) {
		t.Fatalf("FindMissing = %v; want %v", got, want)
	}
}
