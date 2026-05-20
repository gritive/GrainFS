package iamadmin

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// Client speaks to the iamadmin endpoints on the admin HTTP server.
// Transport plumbing lives in adminapi; this type only wires endpoint methods.
type Client struct {
	*adminapi.Transport
}

// NewClient resolves the endpoint (flag value → GRAINFS_ADMIN_SOCKET env →
// fail-fast) and returns a ready-to-use client. Matches the legacy IAM CLI
// resolution order so existing operator muscle memory still works.
func NewClient(endpoint string) (*Client, error) {
	ep, err := ResolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	tp, err := adminapi.NewTransport(ep)
	if err != nil {
		return nil, err
	}
	return &Client{Transport: tp}, nil
}

// NewClientForURL builds a Client against an explicit http(s) base URL
// with no auto-discovery. Used by tests against httptest.Server.
func NewClientForURL(rawurl string) *Client {
	tp, _ := adminapi.NewTransport(rawurl)
	return &Client{Transport: tp}
}

// --- ServiceAccount ---

// SACreate creates a ServiceAccount and returns its first AccessKey + one-time secret.
func (c *Client) SACreate(ctx context.Context, name, description string) (SACreateResponse, error) {
	body := map[string]string{"name": name, "description": description}
	var resp SACreateResponse
	err := c.Post(ctx, "/v1/iam/sa", body, &resp)
	return resp, err
}

// SAList returns every ServiceAccount known to the admin server.
func (c *Client) SAList(ctx context.Context) ([]SAListItem, error) {
	var resp []SAListItem
	err := c.Get(ctx, "/v1/iam/sa", &resp)
	return resp, err
}

// SAGet returns metadata for the named ServiceAccount.
func (c *Client) SAGet(ctx context.Context, saID string) (SAGetResponse, error) {
	var resp SAGetResponse
	err := c.Get(ctx, "/v1/iam/sa/"+url.PathEscape(saID), &resp)
	return resp, err
}

// SADelete removes a ServiceAccount; the server cascades to its keys + grants via FSM.
func (c *Client) SADelete(ctx context.Context, saID string) error {
	return c.Delete(ctx, "/v1/iam/sa/"+url.PathEscape(saID), nil)
}

// --- AccessKey ---

// KeyCreateRaw mirrors the existing CLI behavior: the server response body
// is passed through verbatim, both for text and json modes. Returning []byte
// preserves that semantic.
func (c *Client) KeyCreateRaw(ctx context.Context, saID string, buckets []string) ([]byte, error) {
	body := map[string]any{}
	if len(buckets) > 0 {
		body["buckets"] = buckets
	}
	return c.PostRaw(ctx, "/v1/iam/sa/"+url.PathEscape(saID)+"/key", body)
}

// KeyRevoke revokes a single AccessKey on the named ServiceAccount.
func (c *Client) KeyRevoke(ctx context.Context, saID, accessKey string) error {
	return c.Delete(ctx,
		"/v1/iam/sa/"+url.PathEscape(saID)+"/key/"+url.PathEscape(accessKey), nil)
}

// --- Policy ---

// PolicyPut uploads a custom policy document to the admin server.
func (c *Client) PolicyPut(ctx context.Context, name string, doc []byte) error {
	return c.Do(ctx, "PUT", "/v1/iam/policy/"+url.PathEscape(name), json.RawMessage(doc), nil)
}

// PolicyGet returns the raw policy document bytes for the named policy.
func (c *Client) PolicyGet(ctx context.Context, name string) ([]byte, error) {
	return c.GetRaw(ctx, "/v1/iam/policy/"+url.PathEscape(name))
}

// PolicyList returns the names of all policies known to the admin server.
func (c *Client) PolicyList(ctx context.Context) ([]string, error) {
	var resp []string
	err := c.Get(ctx, "/v1/iam/policy", &resp)
	return resp, err
}

// PolicyDelete deletes the named policy; the server refuses built-in names.
func (c *Client) PolicyDelete(ctx context.Context, name string) error {
	return c.Delete(ctx, "/v1/iam/policy/"+url.PathEscape(name), nil)
}

// PolicyAttachToSA attaches a policy to the named ServiceAccount via Raft.
func (c *Client) PolicyAttachToSA(ctx context.Context, policyName, saID string) error {
	return c.Put(ctx,
		"/v1/iam/policy/"+url.PathEscape(policyName)+"/attach/sa/"+url.PathEscape(saID),
		nil, nil)
}

// PolicyDetachFromSA detaches a policy from the named ServiceAccount via Raft.
func (c *Client) PolicyDetachFromSA(ctx context.Context, policyName, saID string) error {
	return c.Delete(ctx,
		"/v1/iam/policy/"+url.PathEscape(policyName)+"/attach/sa/"+url.PathEscape(saID),
		nil)
}

// PolicySimulate evaluates a hypothetical request against current cluster IAM state.
func (c *Client) PolicySimulate(ctx context.Context, req PolicySimulateRequest) (PolicySimulateResponse, error) {
	var resp PolicySimulateResponse
	err := c.Post(ctx, "/v1/iam/policy/simulate", req, &resp)
	return resp, err
}

// --- Group ---

// GroupCreate creates a named group (empty policies; attach via GroupPolicyAttach).
func (c *Client) GroupCreate(ctx context.Context, name string) error {
	return c.Put(ctx, "/v1/iam/group/"+url.PathEscape(name), nil, nil)
}

// GroupDelete removes the named group via Raft.
func (c *Client) GroupDelete(ctx context.Context, name string) error {
	return c.Delete(ctx, "/v1/iam/group/"+url.PathEscape(name), nil)
}

// GroupMemberAdd adds saID as a member of group via Raft.
func (c *Client) GroupMemberAdd(ctx context.Context, group, saID string) error {
	return c.Put(ctx,
		"/v1/iam/group/"+url.PathEscape(group)+"/member/"+url.PathEscape(saID),
		nil, nil)
}

// GroupMemberRemove removes saID from group via Raft.
func (c *Client) GroupMemberRemove(ctx context.Context, group, saID string) error {
	return c.Delete(ctx,
		"/v1/iam/group/"+url.PathEscape(group)+"/member/"+url.PathEscape(saID),
		nil)
}

// GroupPolicyAttach attaches a policy to a group via Raft.
func (c *Client) GroupPolicyAttach(ctx context.Context, group, policy string) error {
	return c.Put(ctx,
		"/v1/iam/group/"+url.PathEscape(group)+"/policy/"+url.PathEscape(policy),
		nil, nil)
}

// GroupPolicyDetach detaches a policy from a group via Raft.
func (c *Client) GroupPolicyDetach(ctx context.Context, group, policy string) error {
	return c.Delete(ctx,
		"/v1/iam/group/"+url.PathEscape(group)+"/policy/"+url.PathEscape(policy),
		nil)
}

// --- Bucket (iam bucket subtree) ---

// BucketCreate creates a bucket. When both attachSA and attachPolicy are
// non-empty the server routes through MetaCmd 62 (CreateBucketWithPolicyAttach).
func (c *Client) BucketCreate(ctx context.Context, name, attachSA, attachPolicy string) error {
	body := map[string]any{"name": name}
	if attachSA != "" {
		body["attach_sa"] = attachSA
	}
	if attachPolicy != "" {
		body["attach_policy"] = attachPolicy
	}
	return c.Post(ctx, "/v1/buckets", body, nil)
}

// BucketDelete removes the named bucket. force=true forces deletion of all objects first.
func (c *Client) BucketDelete(ctx context.Context, name string, force bool) error {
	path := "/v1/buckets/" + url.PathEscape(name)
	if force {
		path += "?force=true"
	}
	return c.Delete(ctx, path, nil)
}

// BucketList returns all user-facing buckets (__grainfs_* excluded by server).
func (c *Client) BucketList(ctx context.Context) ([]BucketListItem, error) {
	var resp struct {
		Buckets []BucketListItem `json:"buckets"`
	}
	err := c.Get(ctx, "/v1/buckets", &resp)
	return resp.Buckets, err
}

// BucketPolicyPut sends a raw JSON policy document to the server verbatim.
func (c *Client) BucketPolicyPut(ctx context.Context, bucket string, policy []byte) error {
	return c.Put(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/policy", json.RawMessage(policy), nil)
}

// BucketPolicyDelete removes the bucket policy.
func (c *Client) BucketPolicyDelete(ctx context.Context, bucket string) error {
	return c.Delete(ctx, "/v1/buckets/"+url.PathEscape(bucket)+"/policy", nil)
}
