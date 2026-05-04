package main

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
	"path/filepath"
	"strings"
	"time"
)

// adminClient talks to the GrainFS admin Hertz server. The endpoint is either
// a unix:<path> URL (default; talks to <data>/admin.sock) or a regular
// http://host:port URL (for tests).
type adminClient struct {
	httpClient *http.Client
	baseURL    string // "http://unix" or "http://host:port"
}

// cliError is returned by adminClient on non-2xx responses. Code mirrors the
// admin error JSON; main.go translates to the matching exit code.
type cliError struct {
	Code    string         `json:"code"`
	Message string         `json:"error"`
	Details map[string]any `json:"details,omitempty"`
	Status  int            `json:"-"`
}

func (e *cliError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// newAdminClient resolves the endpoint and returns a ready-to-use client.
// endpoint precedence: explicit --endpoint > GRAINFS_ENDPOINT env > auto-discover.
// Auto-discover sources (in order): --data flag, $GRAINFS_DATA, ./grainfs.toml.
func newAdminClient(endpoint string, dataFlag string) (*adminClient, error) {
	ep := endpoint
	if ep == "" {
		ep = os.Getenv("GRAINFS_ENDPOINT")
	}
	if ep == "" {
		var err error
		ep, err = autoDiscoverSocket(dataFlag)
		if err != nil {
			return nil, err
		}
	}
	if strings.HasPrefix(ep, "unix:") {
		sock := strings.TrimPrefix(ep, "unix:")
		return &adminClient{
			httpClient: &http.Client{
				Transport: &http.Transport{
					DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
						var d net.Dialer
						return d.DialContext(ctx, "unix", sock)
					},
				},
				Timeout: 30 * time.Second,
			},
			baseURL: "http://unix",
		}, nil
	}
	return &adminClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    strings.TrimRight(ep, "/"),
	}, nil
}

// autoDiscoverSocket walks the candidate sources for the admin socket and
// returns the first match. On no match, returns DX1 multi-line friendly error.
func autoDiscoverSocket(dataFlag string) (string, error) {
	type cand struct {
		path   string
		source string
	}
	var candidates []cand
	if dataFlag != "" {
		candidates = append(candidates, cand{filepath.Join(dataFlag, "admin.sock"), "from --data flag"})
	}
	if env := os.Getenv("GRAINFS_DATA"); env != "" {
		candidates = append(candidates, cand{filepath.Join(env, "admin.sock"), "from $GRAINFS_DATA"})
	}
	if dir := readGrainfsTomlDataDir(); dir != "" {
		candidates = append(candidates, cand{filepath.Join(dir, "admin.sock"), "from grainfs.toml"})
	}
	for _, c := range candidates {
		if info, err := os.Stat(c.path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return "unix:" + c.path, nil
		}
	}
	tried := []string{}
	for _, c := range candidates {
		tried = append(tried, fmt.Sprintf("%s (%s)", c.path, c.source))
	}
	if len(tried) == 0 {
		tried = []string{"./grainfs.toml (not found), $GRAINFS_DATA (unset), --data (not given)"}
	}
	return "", fmt.Errorf("admin socket not found.\n  Tried: %s\n  Hint:  grainfs <command> --data /path/to/grainfs/data\n         또는 GRAINFS_DATA=/path 환경변수 설정\n         또는 ./grainfs.toml 에 data_dir = \"/path\" 추가",
		strings.Join(tried, "\n         "))
}

// readGrainfsTomlDataDir parses ./grainfs.toml looking for data_dir = "...".
// Returns empty string if file absent or key missing. Minimal parser — does
// not import a full TOML library since the use case is single-line discovery.
func readGrainfsTomlDataDir() string {
	data, err := os.ReadFile("grainfs.toml")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data_dir") {
			continue
		}
		eq := strings.Index(line, "=")
		if eq < 0 {
			continue
		}
		val := strings.TrimSpace(line[eq+1:])
		val = strings.Trim(val, `"' `)
		if val != "" {
			return val
		}
	}
	return ""
}

// get performs an HTTP GET and JSON-decodes the response into out (if non-nil).
func (c *adminClient) get(path string, out any) error {
	return c.do(http.MethodGet, path, nil, out)
}

func (c *adminClient) post(path string, in any, out any) error {
	return c.do(http.MethodPost, path, in, out)
}

func (c *adminClient) delete(path string, out any) error {
	return c.do(http.MethodDelete, path, nil, out)
}

func (c *adminClient) do(method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Connection-level errors are transient; bubble up so main.go maps to exit 5.
		var ne *net.OpError
		if errors.As(err, &ne) {
			return &cliError{Code: "transient", Message: "admin server unreachable: " + err.Error(), Status: 0}
		}
		return err
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
	cerr := &cliError{Status: resp.StatusCode}
	if err := json.Unmarshal(respBody, cerr); err != nil || cerr.Code == "" {
		cerr.Code = "internal"
		cerr.Message = string(respBody)
	}
	return cerr
}
