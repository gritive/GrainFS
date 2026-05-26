package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeSealReader is a static stand-in for *encrypt.DEKKeeper.
type fakeSealReader struct {
	active uint32
	seals  map[uint32]uint64
}

func (f fakeSealReader) ActiveKEKVersion() uint32             { return f.active }
func (f fakeSealReader) SealCountSnapshot() map[uint32]uint64 { return f.seals }

// fakeLeaseReader is a static stand-in for *encrypt.KEKLeaseTracker.
type fakeLeaseReader struct{ leases map[uint32]uint64 }

func (f fakeLeaseReader) Snapshot() map[uint32]uint64 { return f.leases }

// fakeLifecycleReader is a static stand-in for *cluster.MetaFSM's
// RetiredKEKVersionCount.
type fakeLifecycleReader struct{ retired int }

func (f fakeLifecycleReader) RetiredKEKVersionCount() int { return f.retired }

const (
	wantActiveHdr = `# HELP grainfs_kek_active_version Current active cluster KEK version (the version new DEK seals are wrapped under).
# TYPE grainfs_kek_active_version gauge
`
	wantRetiredHdr = `# HELP grainfs_kek_retired_count Number of KEK versions in retiring or pruned lifecycle state.
# TYPE grainfs_kek_retired_count gauge
`
	wantSealHdr = `# HELP grainfs_kek_seal_count Active-AEAD seals attributed to each KEK version (monotonic per version). Monitor the active version against nonce-collision thresholds (warn 1e8, alert 1e9).
# TYPE grainfs_kek_seal_count counter
`
	wantLeaseHdr = `# HELP grainfs_kek_lease_count In-flight KEK consumer leases per version. Must reach 0 before a retired version can be pruned.
# TYPE grainfs_kek_lease_count gauge
`
)

func TestKEKCollector_ActiveRetiredAndLeases(t *testing.T) {
	keeper := fakeSealReader{
		active: 2,
		seals: map[uint32]uint64{
			0: 5,           // older version
			1: 100_000_000, // older version
			2: 42,          // active
		},
	}
	tracker := fakeLeaseReader{leases: map[uint32]uint64{1: 3}}
	// FSM says 2 versions are retiring/pruned.
	lifecycle := fakeLifecycleReader{retired: 2}

	c := NewKEKCollector(keeper, tracker, lifecycle)

	want := "\n" + wantActiveHdr + "grainfs_kek_active_version 2\n" +
		wantRetiredHdr + "grainfs_kek_retired_count 2\n" +
		wantSealHdr +
		"grainfs_kek_seal_count{kek_version=\"0\"} 5\n" +
		"grainfs_kek_seal_count{kek_version=\"1\"} 1e+08\n" +
		"grainfs_kek_seal_count{kek_version=\"2\"} 42\n" +
		wantLeaseHdr +
		"grainfs_kek_lease_count{kek_version=\"1\"} 3\n"
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected metrics: %v", err)
	}
}

// TestKEKCollector_PostRotationNoRetire is the regression guard for the HIGH
// review finding: after V0→V1 rotation with NO operator `retire`, the keeper
// snapshot still carries both versions {0,1}, but the FSM lifecycle reports 0
// retired (V0 is "previous active", not retired). retired_count MUST be 0 —
// driven by the FSM lifecycle source, not len(snapshot)-1.
func TestKEKCollector_PostRotationNoRetire(t *testing.T) {
	keeper := fakeSealReader{
		active: 1,
		seals: map[uint32]uint64{
			0: 5,  // previous active, never retired
			1: 42, // active
		},
	}
	lifecycle := fakeLifecycleReader{retired: 0}

	c := NewKEKCollector(keeper, nil, lifecycle)

	want := "\n" + wantActiveHdr + "grainfs_kek_active_version 1\n" +
		wantRetiredHdr + "grainfs_kek_retired_count 0\n" +
		wantSealHdr +
		"grainfs_kek_seal_count{kek_version=\"0\"} 5\n" +
		"grainfs_kek_seal_count{kek_version=\"1\"} 42\n"
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected metrics: %v", err)
	}
}

func TestKEKCollector_NilLifecycle_OmitsRetiredCount(t *testing.T) {
	keeper := fakeSealReader{active: 0, seals: map[uint32]uint64{0: 0}}
	c := NewKEKCollector(keeper, nil, nil)

	// No lifecycle reader: retired_count omitted entirely (not emitted as 0).
	want := "\n" + wantActiveHdr + "grainfs_kek_active_version 0\n" +
		wantSealHdr +
		"grainfs_kek_seal_count{kek_version=\"0\"} 0\n"
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected metrics: %v", err)
	}
}

func TestKEKCollector_NilKeeper_EmitsNothing(t *testing.T) {
	c := NewKEKCollector(nil, nil, fakeLifecycleReader{retired: 5})
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Fatalf("nil keeper must emit no metrics, got %d", n)
	}
}

func TestRegisterKEKCollector_NilKeeperIsNoOp(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Verify the collector registers cleanly on a private registry.
	if err := reg.Register(NewKEKCollector(fakeSealReader{seals: map[uint32]uint64{0: 0}}, nil, nil)); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Calling RegisterKEKCollector with a nil keeper must be a no-op (no panic).
	RegisterKEKCollector(nil, nil, nil)
}
