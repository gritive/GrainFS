// Package clusteradmin is the admin-plane HTTP client for grainfs cluster
// operations. It is consumed by the CLI (cmd/grainfs) and tests; the server
// side lives in internal/server. Keeping this in internal/ rather than cmd/
// means the wire shapes, pre-flight rules, and rendering helpers are unit-
// testable in isolation and stay reusable from non-CLI callers.
package clusteradmin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// HeaderIfMatchRev is the optimistic-concurrency request header carrying the
// expected cluster-config rev. Centralised here so client + handler + tests
// agree on the exact spelling; a typo on one side would silently disable OCC.
const HeaderIfMatchRev = "If-Match-Rev"

// Client speaks to a single grainfs server's admin endpoints. Transport
// plumbing (UDS/HTTP dispatch, JSON marshal, error envelope) lives in
// adminapi; this type only wires endpoint methods.
type Client struct {
	*adminapi.Transport
}

// NewClient honors the endpoint scheme. The signature returns *Client without
// error to match many existing CLI call sites; an unconstructable Transport
// (impossible after the F1 resolution — empty endpoint is now accepted and
// fails at request time) would surface at the first method call.
func NewClient(endpoint string) *Client {
	tp, _ := adminapi.NewTransport(endpoint)
	return &Client{Transport: tp}
}

// Status fetches /v1/cluster/status. ctx controls the deadline; pass a
// context.WithTimeout to bound the call.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	body, err := c.GetRaw(ctx, "/v1/cluster/status")
	if err != nil {
		return nil, err
	}
	var s Status
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	return &s, nil
}

// StatusRaw fetches /v1/cluster/status and returns the response body
// unchanged. Use this for `cluster status --format json` to preserve
// forward-compatibility: new server fields (not yet in the typed Status
// struct) round-trip without loss.
//
// For text output, prefer Status() which returns a typed struct.
func (c *Client) StatusRaw(ctx context.Context) ([]byte, error) {
	return c.GetRaw(ctx, "/v1/cluster/status")
}

func (c *Client) Placement(ctx context.Context, opts PlacementOptions) (*PlacementReport, error) {
	q := url.Values{}
	if opts.Bucket != "" {
		q.Set("bucket", opts.Bucket)
	}
	if opts.Key != "" {
		q.Set("key", opts.Key)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	path := "/v1/cluster/placement"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var report PlacementReport
	if err := c.Get(ctx, path, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// RemovePeer issues POST /v1/cluster/remove-peer. On non-2xx responses the
// returned error is *RemovePeerError so callers can branch on status code
// and surface server-supplied context.
func (c *Client) RemovePeer(ctx context.Context, id string, force bool) error {
	body := map[string]any{"id": id, "force": force}
	if err := c.Post(ctx, "/v1/cluster/remove-peer", body, nil); err != nil {
		if ae, ok := asAdminError(err); ok {
			return parseRemovePeerError(ae)
		}
		return err
	}
	return nil
}

// EventLog fetches /v1/cluster/eventlog with the given since (lookback
// duration) and limit. The server endpoint is gated by UDS file mode.
func (c *Client) EventLog(ctx context.Context, since time.Duration, limit int) ([]Event, error) {
	path := fmt.Sprintf("/v1/cluster/eventlog?since=%d&limit=%d", int64(since.Seconds()), limit)
	var out []Event
	if err := c.Get(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// TransferLeader issues POST /v1/cluster/transfer-leader. On non-2xx the
// returned error is *TransferLeaderError so callers can branch on Retry.
func (c *Client) TransferLeader(ctx context.Context) (*TransferLeaderResult, error) {
	var out TransferLeaderResult
	if err := c.Post(ctx, "/v1/cluster/transfer-leader", struct{}{}, &out); err != nil {
		if ae, ok := asAdminError(err); ok {
			return nil, parseTransferLeaderError(ae)
		}
		return nil, err
	}
	return &out, nil
}

// Health fetches GET /v1/cluster/health (typed parse).
func (c *Client) Health(ctx context.Context) (*Health, error) {
	var h Health
	if err := c.Get(ctx, "/v1/cluster/health", &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// BalancerStatus fetches GET /v1/cluster/balancer/status (typed parse).
func (c *Client) BalancerStatus(ctx context.Context) (*BalancerStatus, error) {
	var b BalancerStatus
	if err := c.Get(ctx, "/v1/cluster/balancer/status", &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// ClusterConfigGet fetches GET /v1/cluster/config. The returned response
// carries the effective-key map + per-key source ("default" | "explicit")
// used by the `grainfs cluster config show/get/diff` subcommands.
func (c *Client) ClusterConfigGet(ctx context.Context) (*ClusterConfigResponse, error) {
	var out ClusterConfigResponse
	if err := c.Get(ctx, "/v1/cluster/config", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ClusterConfigPatchRawResponse is the raw-body return shape for
// ClusterConfigPatchRaw. Raw is the server's response body verbatim so
// callers (e.g. the CLI) can print future server-side fields without this
// package needing a typed bump. Rev is the parsed `rev` field for callers
// that just want the new revision.
type ClusterConfigPatchRawResponse struct {
	Rev uint64
	Raw []byte
}

// ClusterConfigPatch applies a strongly-typed patch. Prefer this for
// in-process Go callers. For callers that build the body as a free-form map
// (e.g. the CLI parsing `key=value` strings), use ClusterConfigPatchRaw.
//
// expectedRev != 0 sets the If-Match-Rev header for optimistic concurrency.
// Returns the new rev as reported by the server response body.
func (c *Client) ClusterConfigPatch(ctx context.Context, req ClusterConfigPatchRequest, expectedRev uint64) (uint64, error) {
	var resp struct {
		Rev uint64 `json:"rev"`
	}
	headers := ifMatchRevHeaders(expectedRev)
	if err := c.PatchWithHeaders(ctx, "/v1/cluster/config", headers, req, &resp); err != nil {
		return 0, err
	}
	return resp.Rev, nil
}

// ClusterConfigPatchRaw is the free-form variant for callers that build the
// body as a map (the CLI parses `<kebab-key>=<json-value>` pairs into a map
// to avoid kebab→field-name reflection). The map keys must match the kebab
// JSON tags of ClusterConfigPatchRequest; the server unmarshal accepts both.
//
// Returns the server response body verbatim (Raw) plus the parsed Rev so
// CLI callers can pass new server-side fields through without a typed bump.
func (c *Client) ClusterConfigPatchRaw(ctx context.Context, body map[string]any, expectedRev uint64) (*ClusterConfigPatchRawResponse, error) {
	headers := ifMatchRevHeaders(expectedRev)
	raw, err := c.PatchRawWithHeaders(ctx, "/v1/cluster/config", headers, body)
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Rev uint64 `json:"rev"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse patch response: %w", err)
	}
	return &ClusterConfigPatchRawResponse{Rev: parsed.Rev, Raw: raw}, nil
}

func ifMatchRevHeaders(expectedRev uint64) map[string]string {
	if expectedRev == 0 {
		return nil
	}
	return map[string]string{HeaderIfMatchRev: strconv.FormatUint(expectedRev, 10)}
}
