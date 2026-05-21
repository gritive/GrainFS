package nfs4server

// T15 NFS§C: Audit emit tests for NFSv4 mount decisions.
//
// Tests verify that opLookupResolvePending calls the audit hook with
// Source="nfs4", correct SourceIP, and empty SAID for anon (F#39).

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/storage"
)

// captureAuditHook captures S3Events emitted by the NFS4 server's audit hook.
type captureAuditHook struct {
	events []audit.S3Event
}

func (c *captureAuditHook) emit(ev audit.S3Event) {
	c.events = append(c.events, ev)
}

// TestNFSLookupResolvePending_EmitsAudit_NFSSAAllow verifies that when a
// mount-SA LOOKUP is allowed, an audit event with Source="nfs4" is emitted.
func TestNFSLookupResolvePending_EmitsAudit_NFSSAAllow(t *testing.T) {
	cap := &captureAuditHook{}
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(mountsastore.MountSA{Name: "alice-mount", NumericUID: 200001}))

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		WithAuditHook(cap.emit),
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)
	d.clientAddr = "10.0.0.1:12345"

	// First LOOKUP: bucket → (pending)
	res := d.opLookup([]byte("default"))
	require.Equal(t, NFS4_OK, res.Status)

	// Second LOOKUP: mount-SA resolution
	res = d.opLookup([]byte("alice-mount"))
	require.Equal(t, NFS4_OK, res.Status)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "nfs4", ev.Source)
	assert.Equal(t, "alice-mount", ev.SAID)
	assert.Equal(t, "default", ev.Bucket)
	assert.Equal(t, "allow", ev.AuthStatus)
	assert.Equal(t, "10.0.0.1:12345", ev.SourceIP)
	assert.NotZero(t, ev.Ts)
}

// TestNFSLookupResolvePending_EmitsAudit_NFSSADeny verifies that a denied
// mount-SA emits an audit event with AuthStatus="deny", Source="nfs4".
func TestNFSLookupResolvePending_EmitsAudit_NFSSADeny(t *testing.T) {
	cap := &captureAuditHook{}
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(mountsastore.MountSA{Name: "bob-mount", NumericUID: 200002}))

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newDenyAuthorizer()),
		WithAuditHook(cap.emit),
	)
	srv.SetExportsForTest(bucketExport("mybucket"))
	d := newAuthDispatcher(srv)
	d.clientAddr = "192.168.1.1:9999"

	res := d.opLookup([]byte("mybucket"))
	require.Equal(t, NFS4_OK, res.Status)

	res = d.opLookup([]byte("bob-mount"))
	assert.Equal(t, NFS4ERR_ACCESS, res.Status)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "nfs4", ev.Source)
	assert.Equal(t, "bob-mount", ev.SAID)
	assert.Equal(t, "mybucket", ev.Bucket)
	assert.Equal(t, "deny", ev.AuthStatus)
	assert.Equal(t, "192.168.1.1:9999", ev.SourceIP)
}

// TestNFSLookupResolvePending_EmitsAudit_AnonAllow verifies anon allow emits
// audit with SAID="" (F#39 empty-string, NOT "(anonymous)"), Source="nfs4".
func TestNFSLookupResolvePending_EmitsAudit_AnonAllow(t *testing.T) {
	dir := t.TempDir()
	bk, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, bk.CreateBucket(ctx, "mybucket"))
	_, err = bk.PutObject(ctx, "mybucket", "file.txt", bytes.NewReader([]byte("content")), "text/plain")
	require.NoError(t, err)

	cap := &captureAuditHook{}
	msaStore := newTestMountSAStore(t) // empty pool → anon path

	srv := NewServer(bk,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		WithAuditHook(cap.emit),
	)
	srv.SetExportsForTest(bucketExport("mybucket"))
	d := newAuthDispatcher(srv)
	d.clientAddr = "172.16.0.1:1234"

	res := d.opLookup([]byte("mybucket"))
	require.Equal(t, NFS4_OK, res.Status)

	// file.txt exists in backend → anon path confirmed
	res = d.opLookup([]byte("file.txt"))
	require.Equal(t, NFS4_OK, res.Status)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "nfs4", ev.Source)
	assert.Equal(t, "", ev.SAID, "anon SAID must be empty string (F#39), NOT (anonymous)")
	assert.Equal(t, "allow", ev.AuthStatus)
	assert.Equal(t, "172.16.0.1:1234", ev.SourceIP)
}

// TestNFSLookupResolvePending_EmitsAudit_AnonDeny verifies anon denied emits
// audit with SAID="" (F#39), Source="nfs4", AuthStatus="deny".
func TestNFSLookupResolvePending_EmitsAudit_AnonDeny(t *testing.T) {
	dir := t.TempDir()
	bk, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, bk.CreateBucket(ctx, "private"))
	_, err = bk.PutObject(ctx, "private", "secret.txt", bytes.NewReader([]byte("secret")), "text/plain")
	require.NoError(t, err)

	cap := &captureAuditHook{}
	msaStore := newTestMountSAStore(t) // empty pool

	srv := NewServer(bk,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newDenyAuthorizer()),
		WithAuditHook(cap.emit),
	)
	srv.SetExportsForTest(bucketExport("private"))
	d := newAuthDispatcher(srv)
	d.clientAddr = "1.2.3.4:5678"

	res := d.opLookup([]byte("private"))
	require.Equal(t, NFS4_OK, res.Status)

	res = d.opLookup([]byte("secret.txt"))
	assert.Equal(t, NFS4ERR_ACCESS, res.Status)

	require.Len(t, cap.events, 1)
	ev := cap.events[0]
	assert.Equal(t, "nfs4", ev.Source)
	assert.Equal(t, "", ev.SAID, "anon deny SAID must be empty (F#39)")
	assert.Equal(t, "deny", ev.AuthStatus)
}

// TestNFSAudit_NoHook_NoPanic verifies that without an audit hook, the NFS4
// server operates normally without panicking.
func TestNFSAudit_NoHook_NoPanic(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(mountsastore.MountSA{Name: "alice-mount", NumericUID: 1}))

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		// No WithAuditHook
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)
	d.clientAddr = "127.0.0.1:1"

	require.Equal(t, NFS4_OK, d.opLookup([]byte("default")).Status)
	require.Equal(t, NFS4_OK, d.opLookup([]byte("alice-mount")).Status)
}

// validateTimestamp is a helper that checks ev.Ts is a plausible recent timestamp.
func validateTimestamp(t *testing.T, ev audit.S3Event) {
	t.Helper()
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	assert.Greater(t, ev.Ts, cutoff, "Ts should be a plausible recent timestamp")
}
