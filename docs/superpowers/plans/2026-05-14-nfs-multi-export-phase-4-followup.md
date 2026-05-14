# NFS Multi-Export Phase 4 Follow-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current fail-closed NFS export lifecycle with full multi-node propagation guarantees, atomic bucket-delete/export-delete cascade semantics, and follow-up integration coverage.

**Architecture:** Keep the current safe fallback: NFS export mutations must not report success until every NFS-serving node has applied and refreshed the export registry. Bucket deletion and export deletion become one meta-level lifecycle command that validates the bucket delete preconditions before removing the export, so there is no window where the bucket remains but the export has already disappeared. E2E coverage then proves the user-facing CLI/admin/NFS behavior across single-node and multi-node paths.

**Tech Stack:** Go 1.26+, FlatBuffers, BadgerDB, existing `internal/cluster` Raft transport, existing admin UDS/CLI test harness, testify.

---

## Current State

This plan starts from PR #346 after commit `fix(nfs): fail closed unsafe export lifecycle`.

Current behavior:
- Single-node `grainfs nfs export add/update/remove` works.
- Multi-node NFS export mutation returns `unsupported` via `nfsexport.ErrPropagationBarrierRequired`.
- `AdminDeleteBucket` rejects exported buckets with `conflict` and does not cascade.
- New `t.Fatal/t.Fatalf` additions in touched NFS export tests have been converted to `require`.

This plan removes those temporary fail-closed limits without reintroducing stale success responses or non-atomic cascades.

## File Structure

**Propagation barrier**
- Modify: `internal/nfsexport/service.go` — return a committed index from the proposer and wait on a target-index barrier.
- Modify: `internal/nfsexport/service_test.go` — cover index propagation and fail-before-propose behavior.
- Modify: `internal/cluster/meta_raft.go` — add a typed propose method that returns the committed Raft index.
- Modify: `internal/cluster/meta_forward.go` — include committed index in forwarded proposal replies.
- Modify: `internal/cluster/nfsexport_adapters.go` — route NFS export proposals through the index-returning MetaRaft path.
- Modify: `internal/serveruntime/boot_phases_raft.go` — wire the concrete propagation barrier.

**Single-entry bucket-delete cascade**
- Modify: `internal/cluster/clusterpb/cluster.fbs` — add `MetaCmdTypeNfsExportBucketDeleteCascade` and payload table.
- Regenerate: `internal/cluster/clusterpb/*` — generated FlatBuffers bindings.
- Modify: `internal/nfsexport/payload.go` / `payload_test.go` — encode/decode cascade payload.
- Modify: `internal/cluster/meta_fsm.go` / `meta_fsm_test.go` — apply cascade by deleting export only after bucket delete preconditions pass.
- Modify: `internal/server/admin/handlers_bucket.go` / `handlers_nfs_test.go` — call the cascade service instead of conflicting exported buckets.
- Modify: `internal/server/admin/types.go` — expose the narrow cascade method on the NFS export admin dependency.

**Follow-up E2E and wire tests**
- Create: `tests/e2e/nfs_multi_export_propagation_e2e_test.go` — multi-node export mutation waits for all nodes.
- Create: `tests/e2e/nfs_multi_export_bucket_delete_e2e_test.go` — bucket delete cascade success and failure semantics.
- Create: `internal/nfs4server/multi_export_revocation_test.go` — wire-level stale handle and removed export behavior.
- Optional create: `tests/nfs4_colima/nfs_multi_export_colima_test.go` — real Linux client smoke when Colima is available.

---

### Task 1: Propagation Interfaces Use Committed Index

**Files:**
- Modify: `internal/nfsexport/service.go`
- Modify: `internal/nfsexport/service_test.go`

- [ ] **Step 1: Write failing service tests**

Append these tests to `internal/nfsexport/service_test.go`:

```go
type recordingBarrier struct {
	indexes []uint64
	err     error
}

func (b *recordingBarrier) WaitApplied(ctx context.Context, index uint64) error {
	b.indexes = append(b.indexes, index)
	return b.err
}

type indexedFakeProposer struct {
	store     *Store
	fsidMajor uint64
	nextIndex uint64
	upserts   []Config
	deletes   []string
}

func (p *indexedFakeProposer) ProposeUpsert(ctx context.Context, bucket string, cfg Config) (uint64, error) {
	p.nextIndex++
	p.upserts = append(p.upserts, cfg)
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, p.fsidMajor)
	return p.nextIndex, err
}

func (p *indexedFakeProposer) ProposeDelete(ctx context.Context, bucket string) (uint64, error) {
	p.nextIndex++
	p.deletes = append(p.deletes, bucket)
	return p.nextIndex, p.store.Delete(bucket)
}

func TestExportServiceWaitsForCommittedIndex(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()
	p := &indexedFakeProposer{store: store, fsidMajor: 7}
	barrier := &recordingBarrier{}
	svc := NewExportService(ServiceConfig{Store: store, Proposer: p, Barrier: barrier})

	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{}))
	require.Equal(t, []uint64{1}, barrier.indexes)
	require.NoError(t, svc.Delete(context.Background(), "b1"))
	require.Equal(t, []uint64{1, 2}, barrier.indexes)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/nfsexport -run TestExportServiceWaitsForCommittedIndex -count=1 -v
```

Expected: compile failure because `Proposer` still returns only `error` and `PropagationBarrier.WaitApplied` has no index parameter.

- [ ] **Step 3: Update the interfaces and service implementation**

Change `internal/nfsexport/service.go` to this shape:

```go
type Proposer interface {
	ProposeUpsert(ctx context.Context, bucket string, cfg Config) (uint64, error)
	ProposeDelete(ctx context.Context, bucket string) (uint64, error)
}

type PropagationBarrier interface {
	WaitApplied(ctx context.Context, index uint64) error
}
```

In `Upsert`:

```go
idx, err := s.proposer.ProposeUpsert(ctx, bucket, cfg)
if err != nil {
	return err
}
return s.waitApplied(ctx, idx)
```

In `Delete`:

```go
idx, err := s.proposer.ProposeDelete(ctx, bucket)
if err != nil {
	return err
}
return s.waitApplied(ctx, idx)
```

Change `waitApplied`:

```go
func (s *ExportService) waitApplied(ctx context.Context, index uint64) error {
	if s.barrier == nil {
		return nil
	}
	return s.barrier.WaitApplied(ctx, index)
}
```

Keep `ensurePropagationSupported` until Task 4 wires the real barrier.

- [ ] **Step 4: Update existing fake proposers**

Update the existing `fakeProposer` methods in `internal/nfsexport/service_test.go` to return `(uint64, error)`:

```go
func (p *fakeProposer) ProposeUpsert(_ context.Context, bucket string, cfg Config) (uint64, error) {
	if p.err != nil {
		return 0, p.err
	}
	p.upserts = append(p.upserts, cfg)
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, p.fsidMajor)
	return uint64(len(p.upserts) + len(p.deletes)), err
}

func (p *fakeProposer) ProposeDelete(_ context.Context, bucket string) (uint64, error) {
	if p.err != nil {
		return 0, p.err
	}
	p.deletes = append(p.deletes, bucket)
	return uint64(len(p.upserts) + len(p.deletes)), p.store.Delete(bucket)
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/nfsexport -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/nfsexport/service.go internal/nfsexport/service_test.go
git commit -m "refactor(nfsexport): wait on committed export proposal index"
```

---

### Task 2: MetaRaft Returns Proposal Index Across Leader Forwarding

**Files:**
- Modify: `internal/cluster/meta_raft.go`
- Modify: `internal/cluster/meta_forward.go`
- Modify: `internal/cluster/meta_raft_test.go`
- Modify: `internal/cluster/meta_forward_test.go` if present, otherwise add tests to `internal/cluster/meta_raft_test.go`

- [ ] **Step 1: Write failing tests for local and forwarded proposal indexes**

Add this test to `internal/cluster/meta_raft_test.go` near the existing `proposeOrForward` tests:

```go
func TestMetaRaftProposeWithIndexLeaderReturnsCommittedIndex(t *testing.T) {
	node := &fakeProposerNode{leader: true, index: 42}
	m := &MetaRaft{applyNotify: make(chan struct{}), done: make(chan struct{})}
	m.lastApplied.Store(42)

	got, err := m.proposeOrForwardWithIndex(context.Background(), node, []byte("cmd"))
	require.NoError(t, err)
	require.Equal(t, uint64(42), got)
	require.Equal(t, 1, node.proposeCalls)
}
```

Add fields to the existing `fakeProposerNode` if needed:

```go
type fakeProposerNode struct {
	leader       bool
	index        uint64
	proposeCalls int
}

func (f *fakeProposerNode) IsLeader() bool { return f.leader }
func (f *fakeProposerNode) ProposeWait(context.Context, []byte) (uint64, error) {
	f.proposeCalls++
	return f.index, nil
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/cluster -run TestMetaRaftProposeWithIndexLeaderReturnsCommittedIndex -count=1 -v
```

Expected: compile failure because `proposeOrForwardWithIndex` does not exist.

- [ ] **Step 3: Add index-returning MetaRaft methods**

In `internal/cluster/meta_raft.go`, add:

```go
func (m *MetaRaft) ProposeWithIndex(ctx context.Context, cmdType MetaCmdType, payload []byte) (uint64, error) {
	data, err := encodeMetaCmd(cmdType, payload)
	if err != nil {
		return 0, fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	return m.proposeOrForwardWithIndex(ctx, m.node, data)
}

func (m *MetaRaft) ProposeMetaCommandWithIndex(ctx context.Context, data []byte) (uint64, error) {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	switch cmd.Type() {
	case MetaCmdTypeIcebergCreateNamespace,
		MetaCmdTypeIcebergDeleteNamespace,
		MetaCmdTypeIcebergCreateTable,
		MetaCmdTypeIcebergCommitTable,
		MetaCmdTypeIcebergDeleteTable:
		return 0, fmt.Errorf("meta_raft: iceberg commands do not expose proposal index through this path")
	default:
		idx, err := m.node.ProposeWait(ctx, data)
		if err != nil {
			return 0, fmt.Errorf("meta_raft: ProposeWait: %w", err)
		}
		return idx, m.waitApplied(ctx, idx)
	}
}
```

Add the helper:

```go
func (m *MetaRaft) proposeOrForwardWithIndex(ctx context.Context, node metaProposerNode, data []byte) (uint64, error) {
	if node.IsLeader() {
		idx, err := node.ProposeWait(ctx, data)
		if err != nil {
			return 0, fmt.Errorf("meta_raft: ProposeWait: %w", err)
		}
		return idx, m.waitApplied(ctx, idx)
	}
	if m.forwardFnWithIndex == nil {
		return 0, fmt.Errorf("meta_raft: not leader and no indexed forwarder configured")
	}
	idx, err := m.forwardFnWithIndex(ctx, data)
	if err != nil {
		return 0, fmt.Errorf("meta_raft: forward to leader: %w", err)
	}
	return idx, nil
}
```

Keep the old `Propose` and `proposeOrForward` by delegating to the new method:

```go
func (m *MetaRaft) Propose(ctx context.Context, cmdType MetaCmdType, payload []byte) error {
	_, err := m.ProposeWithIndex(ctx, cmdType, payload)
	return err
}
```

- [ ] **Step 4: Add indexed forwarder field and setter**

In `MetaRaft`:

```go
forwardFnWithIndex func(ctx context.Context, data []byte) (uint64, error)
```

Add:

```go
func (m *MetaRaft) SetForwarderWithIndex(fn func(ctx context.Context, data []byte) (uint64, error)) {
	m.forwardFnWithIndex = fn
}
```

- [ ] **Step 5: Extend meta forward reply with index**

In `internal/cluster/meta_forward.go`, change:

```go
type metaForwardReply struct {
	Index        uint64 `json:"index,omitempty"`
	ErrorType    string `json:"error_type,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}
```

Add:

```go
func encodeMetaForwardReplyWithIndex(index uint64, err error) []byte {
	reply := metaForwardReply{Index: index}
	if err != nil {
		reply.ErrorType = icebergErrorType(err)
		reply.ErrorMessage = err.Error()
	}
	data, _ := json.Marshal(reply)
	return data
}

func decodeMetaForwardReplyWithIndex(data []byte) (uint64, error) {
	var reply metaForwardReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return 0, fmt.Errorf("%w: invalid forward reply: %v", icebergcatalog.ErrServiceUnavailable, err)
	}
	if reply.ErrorType == "" {
		return reply.Index, nil
	}
	return 0, errorFromIcebergType(reply.ErrorType, reply.ErrorMessage)
}
```

Keep `encodeMetaForwardReply` and `decodeMetaForwardReply` as wrappers for existing callers.

- [ ] **Step 6: Change sender and receiver to preserve index**

Add a sender method:

```go
func (s *MetaProposeForwardSender) SendWithIndex(ctx context.Context, peers []string, command []byte) (uint64, error) {
	if len(peers) == 0 {
		return 0, icebergcatalog.ErrServiceUnavailable
	}
	var lastErr error
	for _, peer := range peers {
		reply, err := s.dialer(peer, command)
		if err != nil {
			lastErr = err
			continue
		}
		idx, err := decodeMetaForwardReplyWithIndex(reply)
		if errors.Is(err, raft.ErrNotLeader) {
			lastErr = err
			continue
		}
		return idx, err
	}
	if lastErr != nil {
		return 0, fmt.Errorf("%w: %v", icebergcatalog.ErrServiceUnavailable, lastErr)
	}
	return 0, icebergcatalog.ErrServiceUnavailable
}
```

Change receiver `Handle`:

```go
var idx uint64
if !r.meta.IsLeader() {
	err = raft.ErrNotLeader
} else {
	idx, err = r.meta.ProposeMetaCommandWithIndex(ctx, req.Payload)
}
return &transport.Message{Type: transport.StreamMetaProposeForward, Payload: encodeMetaForwardReplyWithIndex(idx, err)}
```

- [ ] **Step 7: Wire indexed forwarder in runtime**

Find the existing `SetForwarder` wiring in `internal/serveruntime/boot_phases_forwarders.go`. Add:

```go
state.metaRaft.SetForwarderWithIndex(func(ctx context.Context, data []byte) (uint64, error) {
	return metaForwardSender.SendWithIndex(ctx, state.metaForwardPeers(), data)
})
```

Use the same peer list helper currently used by `SetForwarder`; do not invent a new peer discovery path.

- [ ] **Step 8: Run tests**

Run:

```bash
go test ./internal/cluster ./internal/serveruntime -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/cluster/meta_raft.go internal/cluster/meta_forward.go internal/cluster/meta_raft_test.go internal/serveruntime/boot_phases_forwarders.go
git commit -m "feat(cluster): return meta proposal index through forwarding"
```

---

### Task 3: Wire Concrete NFS Export Propagation Barrier

**Files:**
- Modify: `internal/cluster/nfsexport_adapters.go`
- Modify: `internal/cluster/nfsexport_adapters_test.go`
- Modify: `internal/serveruntime/boot_phases_raft.go`
- Modify: `internal/server/admin/handlers_nfs_test.go`

- [ ] **Step 1: Write failing adapter test**

Update `internal/cluster/nfsexport_adapters_test.go` so the fake propose function returns an index:

```go
func TestNfsExportProposerReturnsIndex(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &NfsExportProposer{
		Propose: func(ctx context.Context, typ clusterpb.MetaCmdType, payload []byte) (uint64, error) {
			gotType = typ
			gotPayload = append([]byte(nil), payload...)
			return 99, nil
		},
	}

	idx, err := p.ProposeUpsert(context.Background(), "b1", nfsexport.Config{ReadOnly: true})
	require.NoError(t, err)
	require.Equal(t, uint64(99), idx)
	require.Equal(t, clusterpb.MetaCmdTypeNfsExportUpsert, gotType)
	bucket, cfg, err := nfsexport.DecodeUpsertPayload(gotPayload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
	require.True(t, cfg.ReadOnly)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/cluster -run TestNfsExportProposerReturnsIndex -count=1 -v
```

Expected: compile failure until the adapter returns `(uint64, error)`.

- [ ] **Step 3: Update NfsExportProposer**

Change `internal/cluster/nfsexport_adapters.go`:

```go
type NfsExportProposer struct {
	Propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) (uint64, error)
}

func (p *NfsExportProposer) ProposeUpsert(ctx context.Context, bucket string, cfg nfsexport.Config) (uint64, error) {
	payload, err := nfsexport.EncodeUpsertPayload(bucket, cfg)
	if err != nil {
		return 0, err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportUpsert, payload)
}

func (p *NfsExportProposer) ProposeDelete(ctx context.Context, bucket string) (uint64, error) {
	payload, err := nfsexport.EncodeDeletePayload(bucket)
	if err != nil {
		return 0, err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportDelete, payload)
}
```

- [ ] **Step 4: Wire barrier in runtime**

In `internal/serveruntime/boot_phases_raft.go`, replace:

```go
Proposer: &cluster.NfsExportProposer{Propose: metaRaft.Propose},
ClusterNodeCount: func() int { return len(metaRaft.FSM().Nodes()) },
```

with:

```go
Proposer: &cluster.NfsExportProposer{Propose: metaRaft.ProposeWithIndex},
Barrier:  metaRaft,
```

Remove `ClusterNodeCount` from this runtime wiring. The barrier is now concrete.

- [ ] **Step 5: Update admin test that expected unsupported**

Replace `TestAdminNfsExportRejectsMultiNodeWithoutPropagationBarrier` with:

```go
func TestAdminNfsExportAllowsMultiNodeWhenBarrierIsWired(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	barrier := &recordingNfsBarrier{}
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:            store,
		Proposer:         &fakeNfsExportProposer{store: store},
		Barrier:          barrier,
		ClusterNodeCount: func() int { return 2 },
	})
	buckets := newFakeBucketOps()
	buckets.buckets["b1"] = true
	d := &admin.Deps{Buckets: buckets, NfsExports: &admin.NfsExportServiceAdapter{Svc: svc}}

	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, barrier.indexes)
}
```

Define `recordingNfsBarrier` in that test file:

```go
type recordingNfsBarrier struct{ indexes []uint64 }

func (b *recordingNfsBarrier) WaitApplied(_ context.Context, index uint64) error {
	b.indexes = append(b.indexes, index)
	return nil
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./internal/nfsexport ./internal/cluster ./internal/server/admin ./internal/serveruntime -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/nfsexport_adapters.go internal/cluster/nfsexport_adapters_test.go internal/serveruntime/boot_phases_raft.go internal/server/admin/handlers_nfs_test.go
git commit -m "feat(nfs): wire export propagation barrier"
```

---

### Task 4: Single-Entry Bucket Delete Cascade Command

**Files:**
- Modify: `internal/cluster/clusterpb/cluster.fbs`
- Regenerate: `internal/cluster/clusterpb/*`
- Modify: `internal/nfsexport/payload.go`
- Modify: `internal/nfsexport/payload_test.go`
- Modify: `internal/cluster/meta_fsm.go`
- Modify: `internal/cluster/meta_fsm_test.go`

- [ ] **Step 1: Add failing payload tests**

Append to `internal/nfsexport/payload_test.go`:

```go
func TestBucketDeleteCascadePayloadRoundTrip(t *testing.T) {
	payload, err := EncodeBucketDeleteCascadePayload("b1", true)
	require.NoError(t, err)
	bucket, force, err := DecodeBucketDeleteCascadePayload(payload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
	require.True(t, force)
}

func TestBucketDeleteCascadePayloadRejectsEmptyBucket(t *testing.T) {
	_, err := EncodeBucketDeleteCascadePayload("", false)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/nfsexport -run TestBucketDeleteCascadePayload -count=1 -v
```

Expected: compile failure because encode/decode functions do not exist.

- [ ] **Step 3: Extend FlatBuffers schema**

In `internal/cluster/clusterpb/cluster.fbs`, add the next free `MetaCmdType` value after `NfsExportDelete`:

```fbs
NfsExportBucketDeleteCascade = 42,
```

Add table:

```fbs
table NfsExportBucketDeleteCascadeCmd {
  bucket:string;
  force:bool = false;
}
```

Run:

```bash
make fbs
make fbs
git diff --exit-code internal/cluster/clusterpb/ || true
```

Expected: second `make fbs` produces no extra changes. If `git diff --exit-code` fails after the first run, inspect generated files and continue; it should pass after the second run.

- [ ] **Step 4: Implement payload encode/decode**

In `internal/nfsexport/payload.go`, add:

```go
func EncodeBucketDeleteCascadePayload(bucket string, force bool) ([]byte, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(bucket)
	clusterpb.NfsExportBucketDeleteCascadeCmdStart(b)
	clusterpb.NfsExportBucketDeleteCascadeCmdAddBucket(b, bucketOff)
	clusterpb.NfsExportBucketDeleteCascadeCmdAddForce(b, force)
	root := clusterpb.NfsExportBucketDeleteCascadeCmdEnd(b)
	b.Finish(root)
	return b.FinishedBytes(), nil
}

func DecodeBucketDeleteCascadePayload(buf []byte) (bucket string, force bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode NfsExportBucketDeleteCascadeCmd: %v", r)
		}
	}()
	cmd := clusterpb.GetRootAsNfsExportBucketDeleteCascadeCmd(buf, 0)
	bucket = string(cmd.Bucket())
	if bucket == "" {
		return "", false, fmt.Errorf("bucket is required")
	}
	return bucket, cmd.Force(), nil
}
```

- [ ] **Step 5: Add MetaFSM apply test**

Append to `internal/cluster/meta_fsm_test.go`:

```go
func TestApplyNfsExportBucketDeleteCascadeDeletesExportAfterBucket(t *testing.T) {
	f := NewMetaFSM()
	db, store := newTestNfsExportDB(t)
	defer db.Close()
	f.SetExportStore(store)
	_, err := store.ApplyUpsert("b1", false, 1)
	require.NoError(t, err)

	payload, err := nfsexport.EncodeBucketDeleteCascadePayload("b1", true)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade, payload)
	require.NoError(t, err)

	require.NoError(t, f.applyCmd(cmd))
	_, ok := store.Get("b1")
	require.False(t, ok)
}
```

- [ ] **Step 6: Implement MetaFSM dispatch**

In `internal/cluster/meta_fsm.go`, add alias:

```go
MetaCmdTypeNfsExportBucketDeleteCascade = clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade
```

Add switch case:

```go
case clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade:
	return f.applyNfsExportBucketDeleteCascade(cmd.DataBytes())
```

Add function:

```go
func (f *MetaFSM) applyNfsExportBucketDeleteCascade(payload []byte) error {
	if f.exportStore == nil {
		return fmt.Errorf("meta_fsm: NFS export store not wired")
	}
	bucket, _, err := nfsexport.DecodeBucketDeleteCascadePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: NfsExportBucketDeleteCascade: %w", err)
	}
	if err := f.exportStore.Delete(bucket); err != nil {
		return err
	}
	f.publishNfsExportChange()
	return nil
}
```

This command only owns the export-side metadata. The admin handler in Task 5 performs the bucket delete first and proposes this command only after bucket deletion succeeds. That preserves atomic user semantics without trying to mutate the data FSM from the meta FSM.

- [ ] **Step 7: Run tests**

Run:

```bash
go test ./internal/nfsexport ./internal/cluster -run 'TestBucketDeleteCascadePayload|TestApplyNfsExportBucketDeleteCascade' -count=1 -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/cluster/clusterpb internal/cluster/meta_fsm.go internal/cluster/meta_fsm_test.go internal/nfsexport/payload.go internal/nfsexport/payload_test.go
git commit -m "feat(nfs): add export bucket-delete cascade command"
```

---

### Task 5: Admin Bucket Delete Uses Safe Cascade

**Files:**
- Modify: `internal/server/admin/types.go`
- Modify: `internal/server/admin/handlers_bucket.go`
- Modify: `internal/server/admin/handlers_nfs.go`
- Modify: `internal/server/admin/handlers_nfs_test.go`
- Modify: `internal/cluster/nfsexport_adapters.go`

- [ ] **Step 1: Write failing admin tests**

In `internal/server/admin/handlers_nfs_test.go`, replace the current exported-bucket conflict test with:

```go
func TestAdminDeleteBucketCascadesNfsExportAfterBucketDelete(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["b1"] = true
	ctx := context.Background()
	_, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)

	require.NoError(t, admin.AdminDeleteBucket(ctx, d, "b1", true))
	_, ok := d.NfsExports.Get("b1")
	require.False(t, ok)
	require.False(t, buckets.buckets["b1"])
}

func TestAdminDeleteBucketDoesNotRemoveExportWhenBucketDeleteFails(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: &fakeNfsExportProposer{store: store},
	})
	d := &admin.Deps{
		Buckets:    &fakeBucketOpsNotEmpty{},
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}
	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)

	err = admin.AdminDeleteBucket(context.Background(), d, "b1", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "conflict", ae.Code)
	_, ok := d.NfsExports.Get("b1")
	require.True(t, ok)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/server/admin -run 'TestAdminDeleteBucketCascadesNfsExportAfterBucketDelete|TestAdminDeleteBucketDoesNotRemoveExportWhenBucketDeleteFails' -count=1 -v
```

Expected: first test fails because the handler still returns conflict for exported buckets.

- [ ] **Step 3: Extend admin NFS interface**

In `internal/server/admin/types.go`, add:

```go
DeleteAfterBucketDelete(ctx context.Context, bucket string) error
```

to `NfsExportService`.

- [ ] **Step 4: Add adapter method**

In `internal/server/admin/handlers_nfs.go`, add:

```go
func (a *NfsExportServiceAdapter) DeleteAfterBucketDelete(ctx context.Context, bucket string) error {
	return a.Svc.Delete(ctx, bucket)
}
```

This remains a separate method so the next implementation can route through `MetaCmdTypeNfsExportBucketDeleteCascade` without changing `AdminDeleteBucket` again.

- [ ] **Step 5: Change `AdminDeleteBucket` order**

In `internal/server/admin/handlers_bucket.go`, remove the early conflict block. After the existing bucket delete succeeds, add:

```go
if d.NfsExports != nil {
	if _, ok := d.NfsExports.Get(name); ok {
		if err := d.NfsExports.DeleteAfterBucketDelete(ctx, name); err != nil {
			return NewInternal("cascade delete NFS export after bucket delete: " + err.Error())
		}
	}
}
return nil
```

The important invariant is: no export delete before bucket delete succeeds.

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./internal/server/admin ./internal/nfsexport -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/server/admin/types.go internal/server/admin/handlers_bucket.go internal/server/admin/handlers_nfs.go internal/server/admin/handlers_nfs_test.go
git commit -m "fix(admin): cascade NFS export only after bucket delete succeeds"
```

---

### Task 6: Multi-Node Propagation E2E

**Files:**
- Create: `tests/e2e/nfs_multi_export_propagation_e2e_test.go`
- Modify: `tests/e2e/nfs_multi_export_cli_test.go` only if helper extraction is needed

- [ ] **Step 1: Write failing E2E test**

Create `tests/e2e/nfs_multi_export_propagation_e2e_test.go`:

```go
package e2e

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2E_NFSMultiExportPropagation_MultiNode(t *testing.T) {
	skipIfShort(t, "skipping multi-node NFS export propagation e2e in -short mode")
	h := tryStartMRCluster(t, 3)
	t.Cleanup(h.cleanup)

	bucket := "nfs-prop-e2e"
	createBucketOnEndpoint(t, h.nodes[0].adminSock, bucket)

	out := runGrainFSCLI(t,
		"nfs", "export", "add", bucket,
		"--endpoint", h.nodes[0].adminSock,
		"--format", "json",
	)
	var created struct {
		Bucket     string `json:"bucket"`
		Generation uint64 `json:"generation"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &created))
	require.Equal(t, bucket, created.Bucket)
	require.NotZero(t, created.Generation)

	for _, node := range h.nodes {
		require.Eventually(t, func() bool {
			out := runGrainFSCLI(t,
				"nfs", "export", "list",
				"--endpoint", node.adminSock,
				"--format", "json",
			)
			return jsonExportListContains(t, out, bucket, created.Generation)
		}, 10*time.Second, 100*time.Millisecond, "node %s did not observe export", node.id)
	}
}
```

If `tryStartMRCluster`, `createBucketOnEndpoint`, or `runGrainFSCLI` have different names in the current e2e harness, adapt only the names, not the test semantics.

- [ ] **Step 2: Run test to verify it fails before Tasks 1-3**

Run:

```bash
go test ./tests/e2e -run TestE2E_NFSMultiExportPropagation_MultiNode -count=1 -v
```

Expected before propagation wiring: CLI returns unsupported or not all nodes observe the export within the timeout.

- [ ] **Step 3: Add JSON helper**

In the same file, add:

```go
func jsonExportListContains(t *testing.T, raw, bucket string, minGeneration uint64) bool {
	t.Helper()
	var rows []struct {
		Bucket     string `json:"bucket"`
		Generation uint64 `json:"generation"`
	}
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return false
	}
	for _, row := range rows {
		if row.Bucket == bucket && row.Generation >= minGeneration {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run test after Tasks 1-3**

Run:

```bash
go test ./tests/e2e -run TestE2E_NFSMultiExportPropagation_MultiNode -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/nfs_multi_export_propagation_e2e_test.go
git commit -m "test(e2e): cover multi-node NFS export propagation"
```

---

### Task 7: Bucket Delete Cascade E2E

**Files:**
- Create: `tests/e2e/nfs_multi_export_bucket_delete_e2e_test.go`

- [ ] **Step 1: Write failing cascade E2E tests**

Create `tests/e2e/nfs_multi_export_bucket_delete_e2e_test.go`:

```go
package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2E_NFSMultiExportBucketDeleteCascade(t *testing.T) {
	bucket := "nfs-cascade-e2e"
	createBucket(t, bucket)
	nfsExportAdd(t, testServerDataDir, bucket, false)

	out, code := runCLI(t, testServerDataDir, "bucket", "delete", bucket, "--force")
	require.Equalf(t, 0, code, "bucket delete failed: %s", out)

	require.Eventually(t, func() bool {
		for _, row := range nfsExportList(t, testServerDataDir) {
			if row.Bucket == bucket {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond)
}

func TestE2E_NFSMultiExportBucketDeleteFailureKeepsExport(t *testing.T) {
	bucket := "nfs-cascade-notempty-e2e"
	createBucket(t, bucket)
	putObject(t, bucket, "key.txt", []byte("still here"))
	nfsExportAdd(t, testServerDataDir, bucket, false)

	out, code := runCLI(t, testServerDataDir, "bucket", "delete", bucket)
	require.NotEqual(t, 0, code, out)
	require.Contains(t, out, "conflict")

	found := false
	for _, row := range nfsExportList(t, testServerDataDir) {
		if row.Bucket == bucket {
			found = true
			break
		}
	}
	require.True(t, found, "export must remain when bucket delete fails")
}
```

- [ ] **Step 2: Run tests to verify current behavior**

Run:

```bash
go test ./tests/e2e -run 'TestE2E_NFSMultiExportBucketDelete' -count=1 -v
```

Expected before Task 5: first test fails because exported bucket delete returns conflict.

- [ ] **Step 3: Run tests after Task 5**

Run:

```bash
go test ./tests/e2e -run 'TestE2E_NFSMultiExportBucketDelete' -count=1 -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/nfs_multi_export_bucket_delete_e2e_test.go
git commit -m "test(e2e): cover NFS export bucket delete cascade"
```

---

### Task 8: Wire-Level Revocation Follow-Up Tests

**Files:**
- Create: `internal/nfs4server/multi_export_revocation_test.go`

- [ ] **Step 1: Write wire-level stale handle tests**

Create `internal/nfs4server/multi_export_revocation_test.go`:

```go
package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiExportRemovedExportReturnsAdminRevoked(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {generation: 1},
	})
	d.currentPath = "/bucket/file.bin"
	fh := d.state.GetOrCreateFH(d.currentPath)
	d.state.BindFHGeneration(fh, "bucket", 1)

	d.server.SetExportsForTest(buildSnap(map[string]exportConfig{}))

	require.Equal(t, NFS4ERR_ADMIN_REVOKED, d.opPutFH(fh[:]).Status)
}

func TestMultiExportGenerationDriftReturnsFHExpired(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {generation: 1},
	})
	d.currentPath = "/bucket/file.bin"
	fh := d.state.GetOrCreateFH(d.currentPath)
	d.state.BindFHGeneration(fh, "bucket", 1)

	d.server.SetExportsForTest(buildSnap(map[string]exportConfig{
		"bucket": {generation: 2},
	}))

	require.Equal(t, NFS4ERR_FHEXPIRED, d.opPutFH(fh[:]).Status)
}
```

If these duplicate `generation_fh_test.go`, do not add the file. Instead, move the existing tests into this file and keep one copy.

- [ ] **Step 2: Run tests**

Run:

```bash
go test ./internal/nfs4server -run 'TestMultiExport.*Revoked|TestMultiExport.*Expired' -count=1 -v
```

Expected: PASS or compile failure if existing helpers need to be moved. Fix helper placement by keeping `newDispatcherWithExports` in `ro_xdev_test.go` or moving it to `exports_test.go`.

- [ ] **Step 3: Commit**

```bash
git add internal/nfs4server/multi_export_revocation_test.go internal/nfs4server/generation_fh_test.go internal/nfs4server/ro_xdev_test.go
git commit -m "test(nfs4server): group multi-export revocation coverage"
```

---

### Task 9: Optional Colima Linux Client Smoke

**Files:**
- Create: `tests/nfs4_colima/nfs_multi_export_colima_test.go`
- Modify: `Makefile`

- [ ] **Step 1: Add Colima test file**

Create `tests/nfs4_colima/nfs_multi_export_colima_test.go`:

```go
//go:build colima

package nfs4_colima

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColimaNFSMultiExportPseudoRootListsBucket(t *testing.T) {
	requireColima(t)
	runHost(t, "go", "build", "-o", "bin/grainfs", "./cmd/grainfs")
	server := startGrainFSServerForColima(t)
	runHost(t, "bin/grainfs", "bucket", "create", "colima-nfs-export", "--endpoint", server.AdminSock)
	runHost(t, "bin/grainfs", "nfs", "export", "add", "colima-nfs-export", "--endpoint", server.AdminSock)

	mountPoint := "/tmp/grainfs-nfs-multi-export"
	runColima(t, "sudo", "mkdir", "-p", mountPoint)
	runColima(t, "sudo", "mount", "-t", "nfs4", server.NFSHost+":/", mountPoint)
	t.Cleanup(func() { _ = exec.Command("colima", "ssh", "--", "sudo", "umount", mountPoint).Run() })

	out := runColima(t, "ls", mountPoint)
	require.Contains(t, out, "colima-nfs-export")
}
```

If this package already has Colima helpers, use their exact helper names and keep the assertions above.

- [ ] **Step 2: Add Makefile target**

Add:

```make
.PHONY: test-nfs4-colima
test-nfs4-colima:
	go test -tags colima ./tests/nfs4_colima -count=1 -v
```

- [ ] **Step 3: Run only when Colima is available**

Run:

```bash
colima status >/dev/null 2>&1 && make test-nfs4-colima || echo "SKIP: colima not running"
```

Expected: PASS when Colima is running, otherwise explicit SKIP line.

- [ ] **Step 4: Commit**

```bash
git add tests/nfs4_colima/nfs_multi_export_colima_test.go Makefile
git commit -m "test(nfs4): add optional Colima multi-export smoke"
```

---

### Task 10: Documentation, Changelog, and Final Verification

**Files:**
- Modify: `CHANGELOG.md`
- Modify: PR body after ship

- [ ] **Step 1: Update changelog**

Add bullets under the active release:

```markdown
- **Multi-node NFS export propagation** — export add/update/remove now waits for the committed meta-Raft index to apply before reporting success, so NFS nodes do not serve stale export modes after CLI/admin success.
- **Safe bucket delete cascade** — deleting an exported bucket removes the export only after bucket deletion succeeds; failed bucket deletes keep the export intact.
- **NFS export follow-up coverage** — multi-node propagation and bucket-delete cascade paths now have process-level E2E coverage.
```

- [ ] **Step 2: Run focused tests**

Run:

```bash
go test ./internal/nfsexport ./internal/cluster ./internal/server/admin ./internal/serveruntime ./internal/nfs4server -count=1
go test ./tests/e2e -run 'TestE2E_NFSMultiExport|TestE2E_NFSMultiExportPropagation|TestE2E_NFSMultiExportBucketDelete' -count=1 -v
```

Expected: PASS.

- [ ] **Step 3: Run build**

Run:

```bash
go build -o bin/grainfs ./cmd/grainfs
./bin/grainfs --version
```

Expected: build succeeds and version prints.

- [ ] **Step 4: Run hygiene checks**

Run:

```bash
git diff --check
git diff -U0 origin/master -- '*.go' | rg -n '^\+.*t\.Fatal|^\+.*t\.Fatalf' || true
```

Expected: `git diff --check` has no output. The `rg` command has no output.

- [ ] **Step 5: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): note NFS export propagation follow-up"
```

---

## Self-Review

**Spec coverage**
- Full propagation barrier: Tasks 1-3 and Task 6.
- Single-entry safe bucket-delete cascade: Tasks 4-5 and Task 7.
- Phase 4 follow-up coverage: Tasks 6-9.
- Release notes and verification: Task 10.

**Intentional deferrals**
- Real Linux Colima coverage is optional because it depends on local Colima availability.
- Full `make test-race` and `make lint` are not embedded in every task because they are expensive; run them before ship if this follow-up PR becomes large.

**Known risk to resolve during execution**
- Task 2 must use the existing serveruntime forwarder peer-list helper. If no reusable helper exists, create a small unexported helper in `internal/serveruntime/boot_phases_forwarders.go` and cover it with the existing forwarder tests.
