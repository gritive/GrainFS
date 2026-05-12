package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
)

// Transport is the generic admin HTTP transport shared by every admin client
// package (volumeadmin, clusteradmin, ...). It handles endpoint dispatch
// (UDS vs HTTP), JSON marshaling, and converting transport-level + non-2xx
// responses into *Error envelopes.
//
// Endpoint forms accepted by NewTransport:
//   - "http(s)://..."  — direct HTTP dial (test injection)
//   - "unix:<path>"    — UDS dialer (legacy form)
//   - "<bare path>"    — UDS dialer (CLI-facing form)
//
// An empty endpoint produces a UDS Transport with sockPath="". The first
// request attempt fails at dial time with a *Error{Code:"transient"} rather
// than a constructing-time error — this matches the pre-deepening behavior
// of clusteradmin.NewClient which multiple CLI/test sites depend on.
type Transport struct {
	httpClient *http.Client
	baseURL    string
	sockPath   string // populated for UDS transport; "" for HTTP transport
}

// NewTransport constructs a Transport for the given endpoint.
func NewTransport(endpoint string) (*Transport, error) {
	ep := strings.TrimSpace(endpoint)
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return &Transport{
			httpClient: &http.Client{},
			baseURL:    strings.TrimRight(ep, "/"),
		}, nil
	}
	sock := strings.TrimPrefix(ep, "unix:")
	return &Transport{
		httpClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sock)
				},
			},
		},
		baseURL:  "http://unix",
		sockPath: sock,
	}, nil
}

// Get issues GET path and JSON-decodes the 2xx body into out (if non-nil).
func (t *Transport) Get(ctx context.Context, path string, out any) error {
	return t.Do(ctx, http.MethodGet, path, nil, out)
}

// Post issues POST path with optional JSON body and JSON-decoded response.
func (t *Transport) Post(ctx context.Context, path string, in any, out any) error {
	return t.Do(ctx, http.MethodPost, path, in, out)
}

// Delete issues DELETE path with optional JSON-decoded response.
func (t *Transport) Delete(ctx context.Context, path string, out any) error {
	return t.Do(ctx, http.MethodDelete, path, nil, out)
}

// GetRaw issues GET path and returns the raw 2xx body bytes. Use this when
// the caller needs format-preserving output (e.g. cluster status --format json).
func (t *Transport) GetRaw(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, t.wrapTransportError(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}
	return nil, t.parseErrorBody(resp.StatusCode, body)
}

// Do is the shared request executor.
func (t *Transport) Do(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.baseURL+path, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := t.httpClient.Do(req)
	if err != nil {
		return t.wrapTransportError(err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil && len(respBody) > 0 {
			if err := json.Unmarshal(respBody, out); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}
		}
		return nil
	}
	return t.parseErrorBody(resp.StatusCode, respBody)
}

func (t *Transport) parseErrorBody(status int, body []byte) error {
	var wire Error
	if err := json.Unmarshal(body, &wire); err != nil || wire.Code == "" {
		return &Error{Status: status, Code: "internal", Message: string(body)}
	}
	wire.Status = status
	return &wire
}

// wrapTransportError maps transport-level failures (context cancel, dial
// errors) into *Error envelopes so callers can errors.As uniformly.
func (t *Transport) wrapTransportError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return (&Error{Code: "transient", Message: "admin request cancelled: " + err.Error()}).WithCause(err)
	}
	if msg := udsDialHint(err, t.sockPath); msg != "" {
		return (&Error{Code: "transient", Message: msg}).WithCause(err)
	}
	var ne *net.OpError
	if errors.As(err, &ne) {
		return (&Error{Code: "transient", Message: "admin server unreachable: " + err.Error()}).WithCause(err)
	}
	return err
}

// udsDialHint returns an actionable message for known UDS dial failures, or
// empty string for unrelated errors. Replaces clusteradmin.wrapDialError.
// sockPath="" disables UDS-specific hints (HTTP transport callers).
func udsDialHint(err error, sockPath string) string {
	if sockPath == "" {
		return ""
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		return ""
	}
	switch {
	case errors.Is(err, syscall.ENOENT) || errors.Is(err, os.ErrNotExist):
		return fmt.Sprintf("admin socket not found at %s\n"+
			"  Hint: confirm grainfs serve is running with --data <correct-dir>\n"+
			"        check the \"admin endpoint\" line in server startup logs", sockPath)
	case errors.Is(err, syscall.ECONNREFUSED):
		return fmt.Sprintf("admin socket exists but no listener at %s\n"+
			"  Hint: grainfs serve may have crashed; check server logs", sockPath)
	case errors.Is(err, syscall.EACCES) || errors.Is(err, os.ErrPermission):
		return fmt.Sprintf("permission denied on %s\n"+
			"  Hint: confirm group membership matches --admin-group on serve", sockPath)
	}
	return ""
}
