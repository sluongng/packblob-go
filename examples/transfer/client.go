package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sluongng/packblob-go"
	transferpb "github.com/sluongng/packblob-go/examples/transfer/protobuf"
	"github.com/sluongng/packblob-go/protobuf"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	URL        string
	HTTPClient *http.Client
}

type Result struct {
	Digest packblob.Digest
	Mode   string
	// BytesSent counts upload protobuf body bytes, including a missing-base
	// retry, but not FindMissing, HTTP headers, or responses.
	BytesSent int
}

// FindMissing checks content identities, not filenames. Its answer may be stale
// by the time an upload arrives at a server that can evict blobs.
func (c *Client) FindMissing(ctx context.Context, digests []packblob.Digest) ([]packblob.Digest, error) {
	messages := make([]*protobuf.BlobDigest, len(digests))
	for i, digest := range digests {
		messages[i] = digestMessage(digest)
	}
	req := transferpb.FindMissingRequest_builder{Digests: messages}.Build()
	resp := new(transferpb.FindMissingResponse)
	if _, err := c.post(ctx, "/findmissing", req, resp); err != nil {
		return nil, err
	}
	missing := make([]packblob.Digest, 0, len(resp.GetMissing()))
	for _, message := range resp.GetMissing() {
		digest, err := parseDigest(message)
		if err != nil {
			return nil, err
		}
		missing = append(missing, digest)
	}
	return missing, nil
}

// Upload sends target, optionally using base if the server has it. A nil base
// disables negotiation; an empty non-nil base is a valid candidate.
func (c *Client) Upload(ctx context.Context, base, target []byte) (Result, error) {
	result := Result{Digest: packblob.SHA256Digest(target), Mode: "full"}
	if len(base) > MaxFileSize || len(target) > MaxFileSize {
		return result, fmt.Errorf("files must be at most %d bytes", MaxFileSize)
	}
	full := transferpb.UploadRequest_builder{TargetDigest: digestMessage(result.Digest)}.Build()
	full.SetFile(target) // Selecting the oneof also represents an empty file.
	req := full
	if base != nil {
		missing, err := c.FindMissing(ctx, []packblob.Digest{packblob.SHA256Digest(base)})
		if err != nil {
			return result, err
		}
		if len(missing) == 0 {
			delta, err := packblob.EncodeDelta(base, target, 0)
			if err != nil {
				return result, err
			}
			wire, err := delta.MarshalBinary()
			if err != nil {
				return result, err
			}
			message := new(protobuf.StandaloneDelta)
			if err := proto.Unmarshal(wire, message); err != nil {
				return result, err
			}
			candidate := transferpb.UploadRequest_builder{TargetDigest: digestMessage(result.Digest), Delta: message}.Build()
			if proto.Size(candidate) < proto.Size(full) {
				req, result.Mode = candidate, "delta"
			}
		}
	}
	response := new(transferpb.UploadResponse)
	result.BytesSent = proto.Size(req)
	status, err := c.post(ctx, "/upload", req, response)
	if status == http.StatusConflict && result.Mode == "delta" {
		result.Mode = "full"
		result.BytesSent += proto.Size(full)
		_, err = c.post(ctx, "/upload", full, response)
	}
	if err != nil {
		return result, err
	}
	confirmed, err := parseDigest(response.GetTargetDigest())
	if err != nil || confirmed != result.Digest {
		return result, fmt.Errorf("server acknowledged a different or invalid target digest")
	}
	return result, nil
}

func (c *Client) post(ctx context.Context, path string, input, output proto.Message) (int, error) {
	data, err := proto.Marshal(input)
	if err != nil {
		return 0, err
	}
	if len(data) > maxMessageSize {
		return 0, fmt.Errorf("request exceeds message limit")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+path, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/protobuf")
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxMessageSize+1))
	if err != nil {
		return response.StatusCode, err
	}
	if len(body) > maxMessageSize {
		return response.StatusCode, fmt.Errorf("response exceeds message limit")
	}
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, fmt.Errorf("%s: HTTP %d: %s", path, response.StatusCode, strings.TrimSpace(string(body)))
	}
	return response.StatusCode, proto.Unmarshal(body, output)
}
