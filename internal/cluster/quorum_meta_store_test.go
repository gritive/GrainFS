package cluster

import (
	"context"
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
	mu      sync.Mutex
	latest  map[string][]byte // bucket\x00key -> blob
	version map[string][]byte // bucket\x00versionSubpath -> blob
	ops     *opRecorder       // shared ordered write log (per-version vs latest)
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
	return nil, nil
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
	mu     sync.Mutex
	latest map[string][]byte // addr\x00bucket\x00key -> blob
	ops    *opRecorder
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
	if blob, ok := f.latest[addr+"\x00"+fakeKey(bucket, key)]; ok {
		return append([]byte(nil), blob...), nil
	}
	return nil, nil // OK + empty == definitive not-found on that peer
}

func (f *fakePeerQuorumMeta) ReadQuorumMetaVersions(ctx context.Context, addr, bucket, key string) ([]PutObjectMetaCmd, error) {
	return nil, nil
}
func (f *fakePeerQuorumMeta) ReadQuorumMetaVersionsRaw(ctx context.Context, addr, bucket, key string) ([][]byte, error) {
	return nil, nil
}
func (f *fakePeerQuorumMeta) ScanQuorumMeta(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	return nil, nil
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
