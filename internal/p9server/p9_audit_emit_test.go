package p9server

// T15 NFS§C: Audit emit tests for 9P attach decisions.
//
// Tests verify that Walk first-component emits audit events with
// Source="9p", correct SAID (empty for anon, F#39), and AuthStatus.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// captureP9AuditHook captures S3Events emitted by the 9P server's audit hook.
type captureP9AuditHook struct {
	events []audit.S3Event
}

func (c *captureP9AuditHook) emit(ev audit.S3Event) {
	c.events = append(c.events, ev)
}

// allow9PAuthz allows all requests.
type allow9PAuthz struct{}

func (allow9PAuthz) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	return policy.EvalResult{Decision: policy.DecisionAllow, Reason: "test-allow"}
}

// deny9PAuthz denies all requests.
type deny9PAuthz struct{}

func (deny9PAuthz) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "test-deny"}
}

// TestP9Walk_EmitsAudit_MountSAAllow verifies that a mount-SA Walk emits
// an audit event with Source="9p", SAID=<name>, AuthStatus="allow".
func TestP9Walk_EmitsAudit_MountSAAllow(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	cap := &captureP9AuditHook{}
	msaStore := newStubMountSAStore(t, "alice-mount")
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: msaStore,
		authorizer:   allow9PAuthz{},
		auditHook:    cap.emit,
	}

	qids, file, err := root.Walk([]string{"alice-mount@default"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	require.NotNil(t, file)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "9p", ev.Source)
	assert.Equal(t, "alice-mount", ev.SAID)
	assert.Equal(t, "default", ev.Bucket)
	assert.Equal(t, "allow", ev.AuthStatus)
	assert.NotZero(t, ev.Ts)
}

// TestP9Walk_EmitsAudit_MountSADeny verifies that a mount-SA denied Walk emits
// audit with AuthStatus="deny", Source="9p".
func TestP9Walk_EmitsAudit_MountSADeny(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	cap := &captureP9AuditHook{}
	msaStore := newStubMountSAStore(t, "bob-mount")
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: msaStore,
		authorizer:   deny9PAuthz{},
		auditHook:    cap.emit,
	}

	_, _, err := root.Walk([]string{"bob-mount@default"})
	require.Error(t, err)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "9p", ev.Source)
	assert.Equal(t, "bob-mount", ev.SAID)
	assert.Equal(t, "default", ev.Bucket)
	assert.Equal(t, "deny", ev.AuthStatus)
}

// TestP9Walk_EmitsAudit_AnonAllow verifies anon Walk emits audit with
// SAID="" (F#39 empty-string, NOT "(anonymous)"), Source="9p", AuthStatus="allow".
func TestP9Walk_EmitsAudit_AnonAllow(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "mybucket"))

	cap := &captureP9AuditHook{}
	msaStore := newStubMountSAStore(t) // empty pool
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: msaStore,
		authorizer:   allow9PAuthz{},
		auditHook:    cap.emit,
	}

	qids, file, err := root.Walk([]string{"mybucket"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	require.NotNil(t, file)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "9p", ev.Source)
	assert.Equal(t, "", ev.SAID, "anon SAID must be empty string (F#39), NOT (anonymous)")
	assert.Equal(t, "mybucket", ev.Bucket)
	assert.Equal(t, "allow", ev.AuthStatus)
}

// TestP9Walk_EmitsAudit_AnonDeny verifies anon denied Walk emits audit with
// SAID="" (F#39), Source="9p", AuthStatus="deny".
func TestP9Walk_EmitsAudit_AnonDeny(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "private"))

	cap := &captureP9AuditHook{}
	msaStore := newStubMountSAStore(t) // empty pool
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: msaStore,
		authorizer:   deny9PAuthz{},
		auditHook:    cap.emit,
	}

	_, _, err := root.Walk([]string{"private"})
	require.Error(t, err)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "9p", ev.Source)
	assert.Equal(t, "", ev.SAID, "anon deny SAID must be empty (F#39)")
	assert.Equal(t, "deny", ev.AuthStatus)
}

// TestP9Walk_NoAuditHook_NoPanic verifies that without an audit hook, Walk
// operates normally without panicking.
func TestP9Walk_NoAuditHook_NoPanic(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	msaStore := newStubMountSAStore(t, "alice-mount")
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: msaStore,
		authorizer:   allow9PAuthz{},
		// no auditHook
	}

	qids, file, err := root.Walk([]string{"alice-mount@default"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	require.NotNil(t, file)
}
