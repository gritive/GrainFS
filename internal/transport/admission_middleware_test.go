package transport

import (
	"context"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/require"
)

// TestAdmissionMiddleware_RejectsWhenAcquireFails pins that the extracted
// admission middleware actually gates the route: when the limiter cannot admit
// (its class is saturated AND the request ctx is already cancelled, so Acquire
// returns ctx.Err()), the middleware aborts with 503 and does NOT invoke the
// downstream handler. This is the regression guard for the per-handler
// limiter.Acquire boilerplate that moved into admissionMiddleware.
func TestAdmissionMiddleware_RejectsWhenAcquireFails(t *testing.T) {
	tr := &HTTPTransport{}
	tr.SetTrafficLimits(TrafficLimits{Bulk: 1}) // 1-slot Bulk class (StreamShardWriteBody is Bulk)

	// Saturate the Bulk class so a second Acquire must wait.
	release, err := tr.traffic.Acquire(context.Background(), StreamShardWriteBody)
	require.NoError(t, err)
	defer release()

	nextCalled := false
	next := func(c context.Context, ctx *app.RequestContext) { nextCalled = true }

	rc := app.NewContext(0)
	rc.SetHandlers(app.HandlersChain{tr.admissionMiddleware(StreamShardWriteBody), next})

	// Already-cancelled ctx → the saturated Acquire returns immediately with
	// ctx.Err() rather than blocking.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	rc.Next(cctx)

	require.False(t, nextCalled, "handler must not run when admission is rejected")
	require.Equal(t, consts.StatusServiceUnavailable, rc.Response.StatusCode())
}

// TestAdmissionMiddleware_PassesWhenAdmitted pins the happy path: when the
// limiter admits, the middleware calls the downstream handler and releases the
// slot afterward (a follow-up Acquire on the same 1-slot class succeeds).
func TestAdmissionMiddleware_PassesWhenAdmitted(t *testing.T) {
	tr := &HTTPTransport{}
	tr.SetTrafficLimits(TrafficLimits{Bulk: 1})

	nextCalled := false
	next := func(c context.Context, ctx *app.RequestContext) { nextCalled = true }

	rc := app.NewContext(0)
	rc.SetHandlers(app.HandlersChain{tr.admissionMiddleware(StreamShardWriteBody), next})
	rc.Next(context.Background())

	require.True(t, nextCalled, "handler must run when admitted")
	// The slot was released after the handler: a fresh Acquire on the now-free
	// 1-slot class succeeds immediately (it would block forever if still held,
	// failing the test via timeout).
	rel, err := tr.traffic.Acquire(context.Background(), StreamShardWriteBody)
	require.NoError(t, err, "admission slot must be released after the handler")
	rel()
}
