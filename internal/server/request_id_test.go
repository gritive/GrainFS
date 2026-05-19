package server

import (
	"context"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// invokeRequestID runs WithRequestID against a fresh hertz request context
// with the optional incoming X-GrainFS-Request-Id header, returning the rid
// observed inside the downstream handler via RequestIDFromContext + the
// response header dual-write (X-GrainFS-Request-Id and x-amz-request-id).
func invokeRequestID(t *testing.T, incoming string) (ridFromCtx, hdrGrain, hdrAmz string) {
	t.Helper()
	c := app.NewContext(0)
	if incoming != "" {
		c.Request.Header.Set(RequestIDHeader, incoming)
	}
	var seen string
	mw := WithRequestID()
	// app.HandlerFunc(c.Next) terminates the chain when there are no handlers;
	// set a single handler that captures the rid from context.
	c.SetHandlers(app.HandlersChain{
		func(ctx context.Context, c *app.RequestContext) {
			seen = RequestIDFromContext(ctx)
		},
	})
	mw(context.Background(), c)
	return seen, string(c.Response.Header.Get(RequestIDHeader)), string(c.Response.Header.Get("x-amz-request-id"))
}

func TestRequestID_GeneratesIfAbsent(t *testing.T) {
	ridCtx, hdrGrain, hdrAmz := invokeRequestID(t, "")
	require.NotEmpty(t, ridCtx, "RequestIDFromContext should return non-empty when absent")
	require.NotEmpty(t, hdrGrain, "X-GrainFS-Request-Id response header should be set")
	assert.Equal(t, ridCtx, hdrGrain, "ctx and X-GrainFS-Request-Id header must match")
	assert.Equal(t, hdrGrain, hdrAmz, "x-amz-request-id must equal X-GrainFS-Request-Id")

	// Parsed value should be a valid UUID; UUIDv7 has version nibble == 7.
	parsed, err := uuid.Parse(ridCtx)
	require.NoError(t, err, "generated rid must be a valid UUID")
	assert.Equal(t, uuid.Version(7), parsed.Version(), "generated rid must be UUIDv7")
}

func TestRequestID_PreservesIncoming(t *testing.T) {
	const incoming = "client-rid-abc-123"
	ridCtx, hdrGrain, hdrAmz := invokeRequestID(t, incoming)
	assert.Equal(t, incoming, ridCtx)
	assert.Equal(t, incoming, hdrGrain)
	assert.Equal(t, incoming, hdrAmz)
}
