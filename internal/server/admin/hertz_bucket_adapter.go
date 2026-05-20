package admin

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/rs/zerolog/log"
)

var (
	bucketPolicyEnvelopePrefix = []byte(`{"policy":`)
	bucketPolicyEnvelopeSuffix = []byte(`}`)
)

func bucketGetPolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		resp, err := AdminGetBucketPolicy(ctx, d, name)
		if err != nil {
			writeError(c, err)
			return
		}
		if !json.Valid(resp.Policy) {
			log.Error().
				Str("event", "bucket_policy_invalid_stored_json").
				Str("bucket", name).
				Int("bytes", len(resp.Policy)).
				Str("prefix_hex", hex.EncodeToString(policyLogPrefix(resp.Policy))).
				Msg("stored bucket policy is invalid JSON")
			writeError(c, NewInternal("stored bucket policy is invalid JSON"))
			return
		}

		c.SetStatusCode(consts.StatusOK)
		c.SetContentType("application/json")
		if _, err := c.Write(bucketPolicyEnvelopePrefix); err != nil {
			log.Error().Err(err).Str("event", "bucket_policy_response_write_failed").Str("bucket", name).Msg("write bucket policy response prefix")
			return
		}
		if _, err := c.Write(resp.Policy); err != nil {
			log.Error().Err(err).Str("event", "bucket_policy_response_write_failed").Str("bucket", name).Msg("write bucket policy response body")
			return
		}
		if _, err := c.Write(bucketPolicyEnvelopeSuffix); err != nil {
			log.Error().Err(err).Str("event", "bucket_policy_response_write_failed").Str("bucket", name).Msg("write bucket policy response suffix")
			return
		}
	}
}

func policyLogPrefix(data []byte) []byte {
	if len(data) > 16 {
		return data[:16]
	}
	return data
}

func bucketSetPolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req BucketPolicySetReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if req.Policy == nil || bytes.Equal(req.Policy, []byte("null")) {
			writeError(c, NewInvalid("policy field is required"))
			return
		}
		if err := AdminSetBucketPolicy(ctx, d, name, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketDeletePolicyHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		if err := AdminDeleteBucketPolicy(ctx, d, name); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketSetVersioningHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		var req BucketVersioningSetReq
		body := c.Request.Body()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(c, NewInvalid("invalid JSON body: "+err.Error()))
				return
			}
		}
		if err := AdminSetBucketVersioning(ctx, d, name, req); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}

func bucketDeleteHandler(d *Deps) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		force := string(c.Query("force")) == "true"
		if err := AdminDeleteBucket(ctx, d, name, force); err != nil {
			writeError(c, err)
			return
		}
		c.SetStatusCode(consts.StatusNoContent)
	}
}
