# Raft Log Store At-Rest Encryption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Encrypt raft v2 Badger log stores at rest without changing raft command replication or FSM apply semantics.

**Architecture:** Add a node-local raft-store master key sealed under the cluster KEK, pass that key into Badger native encryption for raft v2 stores, and rewrap the key during KEK rotation before old KEKs can be pruned. The raft log bytes stay plaintext in memory and on the wire; only the local Badger files are encrypted.

**Tech Stack:** Go 1.26+, BadgerDB v4 native encryption, existing `encrypt.KEKStore`, `badgerutil.RaftLogOptions`, raft v2 Badger stores.

---

## File Structure

- Create `internal/serveruntime/raft_store_key.go`
  - Load/generate the node-local raft-store master key sidecar.
  - Seal/open the sidecar under `encrypt.KEKStore`.
  - Expose the active sidecar KEK version for prune checks.
- Create `internal/serveruntime/raft_store_key_test.go`
  - Unit tests for sidecar round-trip, wrong context rejection, missing sidecar
    behavior, and rewrap.
- Modify `internal/badgerutil/options.go`
  - Add `RaftLogEncryptedOptions(path, syncWrites, key)` or an equivalent helper
    that layers Badger encryption onto `RaftLogOptions`.
- Modify `internal/badgerutil/options_test.go`
  - Verify helper rejects invalid key sizes and preserves raft durability knobs.
- Modify `internal/cluster/raftfactory.go`
  - Thread an optional raft-store encryption key into `newRaftNodeV2` and
    `openRaftV2Stores`.
- Modify `internal/cluster/*raft*_test.go`
  - Add encrypted raft log reopen coverage using deterministic test keys.
- Modify `internal/serveruntime/boot_phases_raft.go`
  - Load/generate the raft-store key after KEK store and cluster identity are
    available, before `cluster.NewMetaRaft` opens raft v2 stores.
- Modify `internal/cluster/meta_fsm_kek_apply.go` or the serveruntime KEK
  rotation/prune adapter
  - Rewrap the sidecar on KEK rotation.
  - Refuse KEK prune while the sidecar references the pruned version.
- Modify `internal/serveruntime/encryption_key.go` and format-version tests
  - Bump at-rest format and fail loud on existing raft store without sidecar.
- Modify `docs/operators/runbook.md` and `docs/operators/deploy-production-cluster.md`
  - Document `keys.d/raft-store.key.enc` backup and restore requirements.

## Task 1: Badger Encrypted Raft Options Helper

**Files:**
- Modify: `internal/badgerutil/options.go`
- Modify: `internal/badgerutil/options_test.go`

- [ ] **Step 1: Write failing tests for encrypted raft options**

Add tests:

```go
func TestRaftLogEncryptedOptionsRejectsInvalidKeySize(t *testing.T) {
	_, err := RaftLogEncryptedOptions(t.TempDir(), true, []byte("short"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "encryption key")
}

func TestRaftLogEncryptedOptionsOpensEncryptedDB(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, 32)
	opts, err := RaftLogEncryptedOptions(dir, true, key)
	require.NoError(t, err)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	wrong, err := RaftLogEncryptedOptions(dir, true, bytes.Repeat([]byte{0x24}, 32))
	require.NoError(t, err)
	_, err = badger.Open(wrong)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
go test ./internal/badgerutil -run 'TestRaftLogEncryptedOptions' -count=1
```

Expected: FAIL because `RaftLogEncryptedOptions` does not exist.

- [ ] **Step 3: Implement the helper**

Add:

```go
func RaftLogEncryptedOptions(path string, syncWrites bool, key []byte) (badger.Options, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return badger.Options{}, fmt.Errorf("raft log encryption key len = %d, want 16, 24, or 32", len(key))
	}
	return RaftLogOptions(path, syncWrites).WithEncryptionKey(key), nil
}
```

- [ ] **Step 4: Run package tests**

Run:

```bash
go test ./internal/badgerutil -count=1
```

Expected: PASS.

## Task 2: Raft Store Key Sidecar

**Files:**
- Create: `internal/serveruntime/raft_store_key.go`
- Create: `internal/serveruntime/raft_store_key_test.go`

- [ ] **Step 1: Write tests for sidecar round-trip and context binding**

Test cases:

```go
func TestRaftStoreKeyLoadOrCreateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := testKEKStore(t, 3, bytes.Repeat([]byte{0x33}, 32))
	key1, meta1, err := loadOrCreateRaftStoreKey(dir, store, []byte("cluster-1234567"), "node-a", false)
	require.NoError(t, err)
	require.Len(t, key1, 32)
	require.Equal(t, uint32(3), meta1.KEKVersion)

	key2, meta2, err := loadOrCreateRaftStoreKey(dir, store, []byte("cluster-1234567"), "node-a", false)
	require.NoError(t, err)
	require.Equal(t, key1, key2)
	require.Equal(t, meta1.KEKVersion, meta2.KEKVersion)
}

func TestRaftStoreKeyRejectsWrongNodeContext(t *testing.T) {
	dir := t.TempDir()
	store := testKEKStore(t, 0, bytes.Repeat([]byte{0x44}, 32))
	_, _, err := loadOrCreateRaftStoreKey(dir, store, []byte("cluster-1234567"), "node-a", false)
	require.NoError(t, err)
	_, _, err = loadOrCreateRaftStoreKey(dir, store, []byte("cluster-1234567"), "node-b", false)
	require.Error(t, err)
}
```

- [ ] **Step 2: Implement sidecar format**

Use a small versioned JSON envelope:

```go
type raftStoreKeyEnvelope struct {
	Version    uint32 `json:"version"`
	KEKVer     uint32 `json:"kek_ver"`
	Ciphertext []byte `json:"ciphertext"`
}
```

Seal plaintext with `KEKStore.Get(activeVersion)` and AAD from:

```go
func raftStoreKeyAAD(clusterID []byte, nodeID string) []byte {
	// Build length-prefixed fields to avoid boundary ambiguity.
}
```

Write atomically to `keys.d/raft-store.key.enc` with `0600`, directory mode
`0700`, and parent directory fsync.

- [ ] **Step 3: Add rewrap function**

Add:

```go
func rewrapRaftStoreKey(dataDir string, store *encrypt.KEKStore, clusterID []byte, nodeID string) (uint32, error)
```

It opens the current sidecar, decrypts with the recorded KEK version, and reseals
under `store.ActiveVersion()` without changing the plaintext key.

- [ ] **Step 4: Run focused tests**

Run:

```bash
go test ./internal/serveruntime -run 'TestRaftStoreKey' -count=1
```

Expected: PASS.

## Task 3: Thread Encryption Key Into Raft Factory

**Files:**
- Modify: `internal/cluster/raftfactory.go`
- Modify: `internal/cluster/raft_v2_smoke_test.go`
- Modify: `internal/cluster/raftv2_snapshot_trigger_test.go`
- Modify: `internal/serveruntime/raft_v2_smoke_test.go`

- [ ] **Step 1: Add raft factory option type**

Add:

```go
type RaftV2StoreOptions struct {
	EncryptionKey []byte
}
```

Add a new internal constructor:

```go
func newRaftNodeV2WithStoreOptions(rcfg raft.Config, v2StoreDir string, opts RaftV2StoreOptions) (*raftNodeAdapter, func() error, error)
```

Keep existing `newRaftNodeV2` as a wrapper with zero options so tests and callers
continue to compile.

- [ ] **Step 2: Use encrypted Badger options when key is present**

Inside `openRaftV2Stores`, choose:

```go
if len(opts.EncryptionKey) > 0 {
	bopts, err = badgerutil.RaftLogEncryptedOptions(storeDir, true, opts.EncryptionKey)
} else {
	bopts = badgerutil.RaftLogOptions(storeDir, true)
}
```

Use `RaftLogOptions` instead of `SmallOptions` so raft durability knobs are
explicit at the open site.

- [ ] **Step 3: Add reopen test**

Write a test that appends an entry, closes the raft DB, reopens with the same
key, and reads the entry. Then reopen with a wrong key and require an error.

- [ ] **Step 4: Run raft focused tests**

Run:

```bash
go test ./internal/cluster ./internal/raft -run 'RaftV2|BadgerLogStore|Encrypted' -count=1
```

Expected: PASS.

## Task 4: Serveruntime Boot Wiring

**Files:**
- Modify: `internal/serveruntime/boot_state.go`
- Modify: `internal/serveruntime/boot_phases_raft.go`
- Modify: `internal/serveruntime/raft_v2_smoke_test.go`

- [ ] **Step 1: Store loaded key in boot state**

Add fields:

```go
raftStoreKey []byte
raftStoreKeyKEKVer uint32
```

- [ ] **Step 2: Load/generate key before meta raft construction**

In the boot phase that already has `state.kekStore`, `state.clusterID`, and
`state.nodeID`, call:

```go
key, meta, err := loadOrCreateRaftStoreKey(state.cfg.DataDir, state.kekStore, state.clusterID, state.nodeID, raftStoreExists(state.cfg.DataDir))
```

Pass the key into the new `cluster.NewMetaRaft`/factory option.

- [ ] **Step 3: Zeroize after Badger open**

After raft stores are opened, zeroize any temporary key byte slices that are not
owned by Badger options.

- [ ] **Step 4: Run serveruntime focused tests**

Run:

```bash
go test ./internal/serveruntime -run 'Raft|KEK|RaftStoreKey' -count=1
```

Expected: PASS.

## Task 5: KEK Rotation and Prune Safety

**Files:**
- Modify: `internal/serveruntime/boot_phases_raft.go`
- Modify: `internal/cluster/meta_fsm_kek_apply.go` or the existing serveruntime
  KEK admin/rotation adapter
- Add tests near existing KEK rotation/prune tests.

- [ ] **Step 1: Write prune refusal test**

Set up a sidecar sealed under KEK v1 while the store has v1 and v2. Attempt to
prune v1 and require a clear error:

```text
cannot prune KEK v1: raft-store key is still sealed under this version
```

- [ ] **Step 2: Rewrap on KEK rotation**

After a successful local KEK rotation apply, call `rewrapRaftStoreKey` so the
sidecar moves to the active KEK version.

- [ ] **Step 3: Verify prune succeeds after rewrap**

Run the test again after rewrap and require prune to continue.

- [ ] **Step 4: Run KEK tests**

Run:

```bash
go test ./internal/cluster ./internal/serveruntime -run 'KEK|RaftStoreKey|Prune' -count=1
```

Expected: PASS.

## Task 6: Format Boundary and Documentation

**Files:**
- Modify: `internal/serveruntime/encryption_key.go`
- Modify: `internal/serveruntime/encryption_key_test.go`
- Modify: `docs/operators/runbook.md`
- Modify: `docs/operators/deploy-production-cluster.md`
- Modify: `TODOS.md`

- [ ] **Step 1: Bump at-rest format version**

Follow the existing `EnsureBulkCipherFormat` pattern. The first binary that
requires encrypted raft logs must write a new format version and refuse older
directories unless the raft log store is empty.

- [ ] **Step 2: Add loud failure test**

Create a raft-v2 store with a marker/log file and no `keys.d/raft-store.key.enc`.
Boot should fail with an error naming the missing sidecar and explaining that
in-place migration is unsupported.

- [ ] **Step 3: Document backup requirement**

Add operator docs:

```text
Back up `keys/`, `keys.d/raft-store.key.enc`, `cluster.id`, and raft state
together. Restoring encrypted raft logs without the matching raft-store key
sidecar is not supported.
```

- [ ] **Step 4: Run verification**

Run:

```bash
go test ./internal/badgerutil ./internal/cluster ./internal/serveruntime ./internal/raft -count=1
git diff --check
```

Expected: PASS.

## Self-Review

- Spec coverage: tasks cover storage-layer encryption, boot circularity,
  KEK prune safety, format boundary, and operator docs.
- Placeholder scan: no unresolved placeholders or unspecified test steps remain;
  every task names files and commands.
- Type consistency: `RaftV2StoreOptions`, `RaftLogEncryptedOptions`, and
  `loadOrCreateRaftStoreKey` names are used consistently across tasks.
