package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// iamHTTPClient builds an http.Client that dials the admin UDS.
//
// Historical name: this helper used to live in iam.go. It's now consumed by
// bucket / bucket_policy / bucket_versioning / bucket_upstream cmd files as
// well; a follow-up will migrate those to a bucketadmin package and drop this
// shim. Kept here so cmd/grainfs still compiles after the IAM thin-runner
// refactor.
func iamHTTPClient(sock string) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
}

// iamRequest sends method+path+body to the admin UDS at sock and returns body bytes.
// Non-2xx -> error including server message.
func iamRequest(ctx context.Context, sock, method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://unix"+path, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := iamHTTPClient(sock).Do(req)
	if err != nil {
		return nil, fmt.Errorf("admin UDS dial %s: %w", sock, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("admin %s %s -> %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(out)))
	}
	return out, nil
}
