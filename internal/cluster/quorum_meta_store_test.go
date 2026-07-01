package cluster

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// This file exercises QuorumMetaStore's orchestration in isolation: a *QuorumMetaStore
// is constructed with FAKE adapters (fakeLocalQuorumMeta + fakePeerQuorumMeta + a
// fake ShardGroupSource + a fake versioning source + a selfAddr closure), so the
// fan-out write ordering and the LWW read merge are testable WITHOUT a transport or
// a real ShardService. This is the "interface is the test surface" payoff of the
// extraction — the inline DistributedBackend methods could only be tested through a
// live two-node transport.

// --- fakes -------------------------------------------------------------------

// opRecorder is a thread-safe ordered log of fan-out write ops shared by the local
// and peer fakes. The fan-out spawns concurrent goroutines across nodes, so the log
// MUST own its own mutex — appending under each fake's separate mutex would be a
// data race (caught by -race), since those mutexes don't serialize each other.
type opRecorder struct {
	mu  sync.Mutex
	ops []string
}

func (r *opRecorder) record(op string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *opRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

// fakeLocalQuorumMeta is an in-memory localQuorumMetaStore. Latest-only blobs are
// keyed by (bucket,key); per-version blobs by (bucket,versionSubpath). decode uses
// the REAL codec so LWW comparisons over the stored blobs are honest. ops records
// the order of local writes so a test can assert per-version-before-latest ordering.
type fakeLocalQuorumMeta struct {
	mu                sync.Mutex
	latest            map[string][]byte // bucket\x00key -> blob
	version           map[string][]byte // bucket\x00versionSubpath -> blob
	ops               *opRecorder       // shared ordered write log (per-version vs latest)
	scanPageCalls     int
	scanPageEntries   []PutObjectMetaCmd
	scanPageTruncated bool
}

func newFakeLocalQuorumMeta(ops *opRecorder) *fakeLocalQuorumMeta {
	return &fakeLocalQuorumMeta{
		latest:  map[string][]byte{},
		version: map[string][]byte{},
		ops:     ops,
	}
}

func fakeKey(a, b string) string { return a + "\x00" + b }

func (f *fakeLocalQuorumMeta) writeQuorumMetaLocal(bucket, key string, data []byte) error {
	f.ops.record("latest-local")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latest[fakeKey(bucket, key)] = append([]byte(nil), data...)
	return nil
}

func (f *fakeLocalQuorumMeta) writeQuorumMetaLocalWithResult(bucket, key string, data []byte) (quorumMetaLocalWriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := fakeKey(bucket, key)
	result := quorumMetaLocalWriteResult{}
	if before, ok := f.latest[k]; ok {
		result.hadPrevious = true
		result.previous = append([]byte(nil), before...)
		if cand, derr := f.decodeQuorumMetaCmdBlob(data); derr == nil {
			if quorumMetaBlobIsIdempotentReplay(before, data, cand.MetaSeqCAS) {
				return quorumMetaLocalWriteResult{}, nil
			}
			if cur, derr2 := f.decodeQuorumMetaCmdBlob(before); derr2 == nil {
				switch decideQuorumMetaWrite(cur, cand) {
				case quorumMetaWriteRejectCAS:
					return quorumMetaLocalWriteResult{}, errQuorumMetaCASReject
				case quorumMetaWriteSkip:
					return quorumMetaLocalWriteResult{}, nil
				}
			}
		}
	}
	f.ops.record("latest-local")
	f.latest[k] = append([]byte(nil), data...)
	result.applied = true
	return result, nil
}

func (f *fakeLocalQuorumMeta) writeQuorumMetaVersionLocal(bucket, versionSubpath string, data []byte) error {
	f.ops.record("version-local")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version[fakeKey(bucket, versionSubpath)] = append([]byte(nil), data...)
	return nil
}

func (f *fakeLocalQuorumMeta) readQuorumMetaRaw(bucket, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if blob, ok := f.latest[fakeKey(bucket, key)]; ok {
		return append([]byte(nil), blob...), nil
	}
	return nil, storage.ErrObjectNotFound
}

func (f *fakeLocalQuorumMeta) readQuorumMetaVersionsLocal(bucket, key string) ([]PutObjectMetaCmd, error) {
	raws, err := f.readQuorumMetaVersionsRawLocal(bucket, key)
	if err != nil {
		return nil, err
	}
	out := make([]PutObjectMetaCmd, 0, len(raws))
	for _, r := range raws {
		cmd, derr := f.decodeQuorumMetaCmdBlob(r)
		if derr != nil {
			continue
		}
		out = append(out, cmd)
	}
	return out, nil
}

func (f *fakeLocalQuorumMeta) readQuorumMetaVersionsRawLocal(bucket, key string) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := fakeKey(bucket, key) + "/"
	var out [][]byte
	for k, blob := range f.version {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, append([]byte(nil), blob...))
		}
	}
	return out, nil
}

func (f *fakeLocalQuorumMeta) ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]PutObjectMetaCmd, 0)
	for k, blob := range f.latest {
		b, key, ok := splitFakeLatestKey(k)
		if !ok || b != bucket || (prefix != "" && !strings.HasPrefix(key, prefix)) {
			continue
		}
		cmd, err := f.decodeQuorumMetaCmdBlob(blob)
		if err == nil {
			out = append(out, cmd)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
func (f *fakeLocalQuorumMeta) ScanQuorumMetaBucketPage(bucket, prefix, marker string, maxKeys int) ([]PutObjectMetaCmd, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanPageCalls++
	if f.scanPageEntries != nil || f.scanPageTruncated {
		return append([]PutObjectMetaCmd(nil), f.scanPageEntries...), f.scanPageTruncated, nil
	}
	var out []PutObjectMetaCmd
	for k, blob := range f.latest {
		b, key, ok := splitFakeLatestKey(k)
		if !ok || b != bucket || (prefix != "" && !strings.HasPrefix(key, prefix)) || (marker != "" && key <= marker) {
			continue
		}
		cmd, err := f.decodeQuorumMetaCmdBlob(blob)
		if err == nil {
			out = append(out, cmd)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > maxKeys {
		return append([]PutObjectMetaCmd(nil), out[:maxKeys]...), true, nil
	}
	return out, false, nil
}
func (f *fakeLocalQuorumMeta) scanQuorumMetaBucketStrict(bucket string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}
func (f *fakeLocalQuorumMeta) ScanQuorumMetaVersionsBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}
func (f *fakeLocalQuorumMeta) scanQuorumMetaVersionsBucketAllStrict(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}

func (f *fakeLocalQuorumMeta) deleteQuorumMetaLocal(bucket, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.latest, fakeKey(bucket, key))
	return nil
}

func (f *fakeLocalQuorumMeta) rollbackQuorumMetaLocalIfMatch(bucket, key string, expected []byte, previous []byte, hadPrevious bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := fakeKey(bucket, key)
	cur, ok := f.latest[k]
	if !ok {
		return nil
	}
	if !bytes.Equal(cur, expected) {
		return nil
	}
	if !hadPrevious {
		delete(f.latest, k)
		return nil
	}
	f.latest[k] = append([]byte(nil), previous...)
	return nil
}

func (f *fakeLocalQuorumMeta) decodeQuorumMetaBlob(data []byte) (*storage.Object, PlacementMeta, error) {
	cmd, err := decodeQuorumMetaBlob(data)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	obj, pm := objectAndPlacementFromCmd(cmd)
	return obj, pm, nil
}

func (f *fakeLocalQuorumMeta) decodeQuorumMetaCmdBlob(data []byte) (PutObjectMetaCmd, error) {
	return decodeQuorumMetaBlob(data)
}

// fakePeerQuorumMeta is an in-memory quorumMetaPeerRPC keyed by addr. It models a
// single remote replica's latest-only blob store. resolvePeerAddress is identity
// (node ID == addr). Decode uses the real codec.
type fakePeerQuorumMeta struct {
	mu                sync.Mutex
	latest            map[string][]byte // addr\x00bucket\x00key -> blob
	ops               *opRecorder
	readRawCalls      int
	readRawBatchCalls int
	scanFullCalls     int
	scanPageCalls     int
}

func newFakePeerQuorumMeta(ops *opRecorder) *fakePeerQuorumMeta {
	return &fakePeerQuorumMeta{latest: map[string][]byte{}, ops: ops}
}

func (f *fakePeerQuorumMeta) resolvePeerAddress(peer string) (string, error) { return peer, nil }

func (f *fakePeerQuorumMeta) WriteQuorumMeta(ctx context.Context, addr, bucket, key string, data []byte) error {
	f.ops.record("latest-peer")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latest[addr+"\x00"+fakeKey(bucket, key)] = append([]byte(nil), data...)
	return nil
}

func (f *fakePeerQuorumMeta) WriteQuorumMetaVersion(ctx context.Context, addr, bucket, versionSubpath string, data []byte) error {
	f.ops.record("version-peer")
	return nil
}

// putPeerLatest seeds a peer's latest-only blob (read-side fixtures).
func (f *fakePeerQuorumMeta) putPeerLatest(addr, bucket, key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latest[addr+"\x00"+fakeKey(bucket, key)] = append([]byte(nil), data...)
}

func (f *fakePeerQuorumMeta) ReadQuorumMetaRaw(ctx context.Context, addr, bucket, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readRawCalls++
	if blob, ok := f.latest[addr+"\x00"+fakeKey(bucket, key)]; ok {
		return append([]byte(nil), blob...), nil
	}
	return nil, nil // OK + empty == definitive not-found on that peer
}

func (f *fakePeerQuorumMeta) ReadQuorumMetaRawBatch(ctx context.Context, addr, bucket string, keys []string) (map[string][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readRawBatchCalls++
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if blob, ok := f.latest[addr+"\x00"+fakeKey(bucket, key)]; ok {
			out[key] = append([]byte(nil), blob...)
		}
	}
	return out, nil
}

func (f *fakePeerQuorumMeta) ReadQuorumMetaVersions(ctx context.Context, addr, bucket, key string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}
func (f *fakePeerQuorumMeta) ReadQuorumMetaVersionsRaw(ctx context.Context, addr, bucket, key string) ([][]byte, error) {
	return nil, nil
}
func (f *fakePeerQuorumMeta) ScanQuorumMeta(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanFullCalls++
	return f.scanLocked(addr, bucket, prefix, "", 0), nil
}
func (f *fakePeerQuorumMeta) ScanQuorumMetaPage(ctx context.Context, addr, bucket, prefix, marker string, maxKeys int) ([]PutObjectMetaCmd, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanPageCalls++
	out := f.scanLocked(addr, bucket, prefix, marker, maxKeys)
	truncated := maxKeys > 0 && len(f.scanLocked(addr, bucket, prefix, marker, 0)) > maxKeys
	return out, truncated, nil
}
func (f *fakePeerQuorumMeta) ScanQuorumMetaVersions(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}
func (f *fakePeerQuorumMeta) ScanQuorumMetaVersionsAll(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}
func (f *fakePeerQuorumMeta) DeleteQuorumMeta(ctx context.Context, addr, bucket, key string) error {
	return nil
}

func (f *fakePeerQuorumMeta) scanLocked(addr, bucket, prefix, marker string, maxKeys int) []PutObjectMetaCmd {
	out := make([]PutObjectMetaCmd, 0)
	for k, blob := range f.latest {
		a, b, key, ok := splitFakePeerLatestKey(k)
		if !ok || a != addr || b != bucket || (prefix != "" && !strings.HasPrefix(key, prefix)) || (marker != "" && key <= marker) {
			continue
		}
		cmd, err := decodeQuorumMetaBlob(blob)
		if err == nil {
			out = append(out, cmd)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if maxKeys > 0 && len(out) > maxKeys {
		return append([]PutObjectMetaCmd(nil), out[:maxKeys]...)
	}
	return out
}

func splitFakeLatestKey(k string) (bucket, key string, ok bool) {
	parts := strings.SplitN(k, "\x00", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func splitFakePeerLatestKey(k string) (addr, bucket, key string, ok bool) {
	parts := strings.SplitN(k, "\x00", 3)
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// fakeVersioning is a 1-method versioningSource.
type fakeVersioning struct{ state string }

func (f fakeVersioning) GetBucketVersioning(bucket string) (string, error) { return f.state, nil }

// fake interfaces are satisfied (catches a signature drift the var-_ guards on
// *ShardService would not, since the fakes are an independent implementation).
var (
	_ localQuorumMetaStore = (*fakeLocalQuorumMeta)(nil)
	_ quorumMetaPeerRPC    = (*fakePeerQuorumMeta)(nil)
	_ versioningSource     = fakeVersioning{}
)

// newFakeQuorumMetaStore wires a QuorumMetaStore over the fakes. selfAddr is fixed.
func newFakeQuorumMetaStore(local *fakeLocalQuorumMeta, peer *fakePeerQuorumMeta, groups ShardGroupSource, versioning string, self string, multiGen bool) *QuorumMetaStore {
	mg := &atomic.Bool{}
	mg.Store(multiGen)
	return &QuorumMetaStore{
		local:      func() localQuorumMetaStore { return local },
		peer:       func() quorumMetaPeerRPC { return peer },
		groups:     func() ShardGroupSource { return groups },
		versioning: fakeVersioning{state: versioning},
		selfAddr:   func() string { return self },
		multiGen:   mg,
	}
}

// --- tests -------------------------------------------------------------------

// TestQuorumMetaStore_WriteOrdersPerVersionBeforeLatest proves the durability
// ordering invariant of writeQuorumMeta: for a versioning-enabled bucket the
// immutable per-version blob is fully written to its K-of-N quorum BEFORE the
// latest-only blob is published. (A latest blob published before its per-version
// authority would point at data a failed per-version write could not roll back.)
// Exercised with no transport — the fake op log records the order across self + a
// peer.
func TestQuorumMetaStore_WriteOrdersPerVersionBeforeLatest(t *testing.T) {
	ops := &opRecorder{}
	local := newFakeLocalQuorumMeta(ops)
	peer := newFakePeerQuorumMeta(ops)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	s := newFakeQuorumMetaStore(local, peer, groups, "Enabled", self, false)

	// ECData=2 (k == node count) makes each fan-out wait for BOTH nodes to ack
	// before returning, so the per-version quorum is fully established before the
	// latest-only fan-out starts — a deterministic ordering assertion (a smaller k
	// would let a best-effort straggler version write race the next phase, which is
	// correct K-of-N behavior but not deterministically orderable in a log).
	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", VersionID: "019ed400-0000-7000-8000-000000000001",
		ModTime: 100, NodeIDs: []string{self, remote}, ECData: 2,
	}
	require.NoError(t, s.writeQuorumMeta(context.Background(), cmd))

	// Every version-* write must precede every latest-* write.
	opLog := ops.snapshot()
	lastVersionIdx, firstLatestIdx := -1, len(opLog)
	for i, op := range opLog {
		switch op[:7] {
		case "version":
			lastVersionIdx = i
		case "latest-":
			if i < firstLatestIdx {
				firstLatestIdx = i
			}
		}
	}
	require.NotEqual(t, -1, lastVersionIdx, "a per-version blob must be written for a versioned PUT")
	require.Less(t, lastVersionIdx, firstLatestIdx,
		"per-version blob must be written BEFORE the latest-only blob (op log: %v)", opLog)

	// And the latest-only blob is now readable locally (the write completed).
	raw, err := s.readQuorumMetaWinningRaw("b", "k")
	require.NoError(t, err)
	got, err := s.local().decodeQuorumMetaCmdBlob(raw)
	require.NoError(t, err)
	require.Equal(t, "k", got.Key)
}

func TestQuorumMetaStoreScatterGatherListPage_UsesLocalPageForSinglePeer(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	local.scanPageEntries = []PutObjectMetaCmd{{Bucket: "bkt", Key: "obj/001"}}
	qms := newFakeQuorumMetaStore(local, newFakePeerQuorumMeta(nil), nil, "", "self", false)

	got, truncated, err := qms.scatterGatherListPage(context.Background(), "bkt", "obj/", "", 1)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Equal(t, []PutObjectMetaCmd{{Bucket: "bkt", Key: "obj/001"}}, got)
	require.Equal(t, 1, local.scanPageCalls)
}

func TestQuorumMetaStoreScatterGatherListPage_UsesPeerPagesForMultiPeer(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	peer := newFakePeerQuorumMeta(nil)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	qms := newFakeQuorumMetaStore(local, peer, groups, "", self, false)

	encode := func(cmd PutObjectMetaCmd) []byte {
		blob, err := encodeQuorumMetaBlob(cmd)
		require.NoError(t, err)
		return blob
	}
	require.NoError(t, local.writeQuorumMetaLocal("bkt", "obj/001", encode(PutObjectMetaCmd{
		Bucket: "bkt", Key: "obj/001", ETag: "local-1", ModTime: 10,
	})))
	peer.putPeerLatest(remote, "bkt", "obj/002", encode(PutObjectMetaCmd{
		Bucket: "bkt", Key: "obj/002", ETag: "remote-2", ModTime: 20,
	}))

	got, truncated, err := qms.scatterGatherListPage(context.Background(), "bkt", "obj/", "", 2)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, got, 2)
	require.Equal(t, "obj/001", got[0].Key)
	require.Equal(t, "obj/002", got[1].Key)
	require.Equal(t, 0, peer.scanFullCalls, "multi-peer page path must not fall back to full prefix scan")
	require.Greater(t, peer.scanPageCalls, 0, "multi-peer page path must scan remote peers by page")
	require.Greater(t, local.scanPageCalls, 0, "multi-peer page path must scan local store by page")
}

func TestQuorumMetaStoreScatterGatherListPage_VerifiesCandidateWinnerAcrossPeers(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	peer := newFakePeerQuorumMeta(nil)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	qms := newFakeQuorumMetaStore(local, peer, groups, "", self, false)

	encode := func(cmd PutObjectMetaCmd) []byte {
		blob, err := encodeQuorumMetaBlob(cmd)
		require.NoError(t, err)
		return blob
	}
	require.NoError(t, local.writeQuorumMetaLocal("bkt", "obj/001", encode(PutObjectMetaCmd{
		Bucket: "bkt", Key: "obj/001", ETag: "deleted", ModTime: 20, IsDeleteMarker: true,
	})))
	peer.putPeerLatest(remote, "bkt", "obj/001", encode(PutObjectMetaCmd{
		Bucket: "bkt", Key: "obj/001", ETag: "stale-live", ModTime: 10,
	}))
	peer.putPeerLatest(remote, "bkt", "obj/002", encode(PutObjectMetaCmd{
		Bucket: "bkt", Key: "obj/002", ETag: "remote-2", ModTime: 30,
	}))

	got, truncated, err := qms.scatterGatherListPage(context.Background(), "bkt", "obj/", "", 1)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, got, 1)
	require.Equal(t, "obj/002", got[0].Key)
}

func TestQuorumMetaStoreScatterGatherListPage_BatchesCandidateWinnerReads(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	peer := newFakePeerQuorumMeta(nil)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	qms := newFakeQuorumMetaStore(local, peer, groups, "", self, false)

	encode := func(cmd PutObjectMetaCmd) []byte {
		blob, err := encodeQuorumMetaBlob(cmd)
		require.NoError(t, err)
		return blob
	}
	for i, key := range []string{"obj/001", "obj/002", "obj/003"} {
		peer.putPeerLatest(remote, "bkt", key, encode(PutObjectMetaCmd{
			Bucket: "bkt", Key: key, ETag: key, ModTime: int64(10 + i),
		}))
	}

	got, truncated, err := qms.scatterGatherListPage(context.Background(), "bkt", "obj/", "", 2)
	require.NoError(t, err)
	require.True(t, truncated)
	require.Len(t, got, 2)
	require.Equal(t, 0, peer.readRawCalls, "candidate winner verification should not issue one raw read RPC per key")
	require.Greater(t, peer.readRawBatchCalls, 0, "candidate winner verification should batch peer raw reads")
}

// TestQuorumMetaStore_NonVersionedSkipsPerVersionBlob proves the per-version blob
// is gated on versioning-enabled: a non-versioned bucket writes ONLY the
// latest-only blob (no version-* op in the log).
func TestQuorumMetaStore_NonVersionedSkipsPerVersionBlob(t *testing.T) {
	ops := &opRecorder{}
	local := newFakeLocalQuorumMeta(ops)
	peer := newFakePeerQuorumMeta(ops)
	const self = "self"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self}},
	}}
	s := newFakeQuorumMetaStore(local, peer, groups, "Unversioned", self, false)

	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", VersionID: "019ed400-0000-7000-8000-000000000002",
		ModTime: 100, NodeIDs: []string{self}, ECData: 1,
	}
	require.NoError(t, s.writeQuorumMeta(context.Background(), cmd))

	opLog := ops.snapshot()
	for _, op := range opLog {
		require.NotContains(t, op, "version", "non-versioned PUT must not write a per-version blob (op log: %v)", opLog)
	}
	require.Contains(t, opLog, "latest-local", "the latest-only blob must still be written")
}

// TestQuorumMetaStore_WinningRaw_MultiGenLWWMerge proves the multi-generation read
// merge: when the local blob and a peer blob disagree, readQuorumMetaWinningRaw
// returns the LWW winner (higher ModTime), not the local copy. This is the
// cross-generation correctness the multiGeneration flag arms — and it is now
// testable with two fake stores and no transport.
func TestQuorumMetaStore_WinningRaw_MultiGenLWWMerge(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	peer := newFakePeerQuorumMeta(nil)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	// multiGeneration ON: the local blob is merged with the peer fan-out.
	s := newFakeQuorumMetaStore(local, peer, groups, "Enabled", self, true)

	// Local holds the STALE blob (lower ModTime); the peer holds the FRESH winner.
	staleBlob := mustEncodeBlob(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v-stale", ModTime: 100, ETag: "stale"})
	freshBlob := mustEncodeBlob(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v-fresh", ModTime: 200, ETag: "fresh"})
	require.NoError(t, local.writeQuorumMetaLocal("b", "k", staleBlob))
	peer.putPeerLatest(remote, "b", "k", freshBlob)

	raw, err := s.readQuorumMetaWinningRaw("b", "k")
	require.NoError(t, err)
	won, err := s.local().decodeQuorumMetaCmdBlob(raw)
	require.NoError(t, err)
	require.Equal(t, int64(200), won.ModTime, "higher-ModTime peer blob must win the cross-generation merge")
	require.Equal(t, "fresh", won.ETag)
}

// TestQuorumMetaStore_WinningRaw_SingleGenLocalFirst proves the default
// (multiGeneration OFF) path is local-first: a present local blob is returned
// verbatim and the peer fan-out is NOT consulted (no merge), byte-identical to the
// legacy fast path. The peer holds a higher-ModTime blob that MUST be ignored.
func TestQuorumMetaStore_WinningRaw_SingleGenLocalFirst(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	peer := newFakePeerQuorumMeta(nil)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	s := newFakeQuorumMetaStore(local, peer, groups, "Enabled", self, false) // multiGen OFF

	localBlob := mustEncodeBlob(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v-local", ModTime: 100, ETag: "local"})
	peerBlob := mustEncodeBlob(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v-peer", ModTime: 999, ETag: "peer"})
	require.NoError(t, local.writeQuorumMetaLocal("b", "k", localBlob))
	peer.putPeerLatest(remote, "b", "k", peerBlob)

	raw, err := s.readQuorumMetaWinningRaw("b", "k")
	require.NoError(t, err)
	won, err := s.local().decodeQuorumMetaCmdBlob(raw)
	require.NoError(t, err)
	require.Equal(t, "local", won.ETag, "single-generation read must return the local blob verbatim (no peer merge)")
}

// TestQuorumMetaStore_WinningRaw_LocalMissFallsBackToPeer proves that on a local
// miss the single-generation path falls back to the peer fan-out and returns the
// peer blob (parity node that missed the K-of-N latest-only write).
func TestQuorumMetaStore_WinningRaw_LocalMissFallsBackToPeer(t *testing.T) {
	local := newFakeLocalQuorumMeta(nil)
	peer := newFakePeerQuorumMeta(nil)
	const self, remote = "self", "peer-1"
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g": {ID: "g", PeerIDs: []string{self, remote}},
	}}
	s := newFakeQuorumMetaStore(local, peer, groups, "Enabled", self, false)

	peerBlob := mustEncodeBlob(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v-peer", ModTime: 200, ETag: "peer"})
	peer.putPeerLatest(remote, "b", "k", peerBlob)

	raw, err := s.readQuorumMetaWinningRaw("b", "k")
	require.NoError(t, err)
	won, err := s.local().decodeQuorumMetaCmdBlob(raw)
	require.NoError(t, err)
	require.Equal(t, "peer", won.ETag, "local miss must fall back to the peer blob")

	// A key absent everywhere is ErrObjectNotFound.
	_, err = s.readQuorumMetaWinningRaw("b", "absent")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func mustEncodeBlob(t *testing.T, cmd PutObjectMetaCmd) []byte {
	t.Helper()
	blob, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)
	return blob
}
