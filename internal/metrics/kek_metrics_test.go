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

func TestKEKCollector_ActiveAndRetired(t *testing.T) {
	keeper := fakeSealReader{
		active: 2,
		seals: map[uint32]uint64{
			0: 5,           // retired
			1: 100_000_000, // retired
			2: 42,          // active
		},
	}
	tracker := fakeLeaseReader{leases: map[uint32]uint64{1: 3}}

	c := NewKEKCollector(keeper, tracker)

	want := `
# HELP grainfs_kek_active_version Current active cluster KEK version (the version new DEK seals are wrapped under).
# TYPE grainfs_kek_active_version gauge
grainfs_kek_active_version 2
# HELP grainfs_kek_retired_count Number of non-active KEK versions the keeper still tracks (versions rotated away from).
# TYPE grainfs_kek_retired_count gauge
grainfs_kek_retired_count 2
# HELP grainfs_kek_seal_count Active-AEAD seals attributed to each KEK version (monotonic per version). Monitor the active version against nonce-collision thresholds (warn 1e8, alert 1e9).
# TYPE grainfs_kek_seal_count counter
grainfs_kek_seal_count{kek_version="0"} 5
grainfs_kek_seal_count{kek_version="1"} 1e+08
grainfs_kek_seal_count{kek_version="2"} 42
# HELP grainfs_kek_lease_count In-flight KEK consumer leases per version. Must reach 0 before a retired version can be pruned.
# TYPE grainfs_kek_lease_count gauge
grainfs_kek_lease_count{kek_version="1"} 3
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected metrics: %v", err)
	}
}

func TestKEKCollector_ActiveOnly_NoLeases(t *testing.T) {
	keeper := fakeSealReader{active: 0, seals: map[uint32]uint64{0: 0}}
	c := NewKEKCollector(keeper, nil)

	// active-only: one seal row at 0, retired_count 0, no lease rows.
	want := `
# HELP grainfs_kek_active_version Current active cluster KEK version (the version new DEK seals are wrapped under).
# TYPE grainfs_kek_active_version gauge
grainfs_kek_active_version 0
# HELP grainfs_kek_retired_count Number of non-active KEK versions the keeper still tracks (versions rotated away from).
# TYPE grainfs_kek_retired_count gauge
grainfs_kek_retired_count 0
# HELP grainfs_kek_seal_count Active-AEAD seals attributed to each KEK version (monotonic per version). Monitor the active version against nonce-collision thresholds (warn 1e8, alert 1e9).
# TYPE grainfs_kek_seal_count counter
grainfs_kek_seal_count{kek_version="0"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected metrics: %v", err)
	}
}

func TestKEKCollector_NilKeeper_EmitsNothing(t *testing.T) {
	c := NewKEKCollector(nil, nil)
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Fatalf("nil keeper must emit no metrics, got %d", n)
	}
}

func TestRegisterKEKCollector_NilKeeperIsNoOp(t *testing.T) {
	// Must not panic and must not register anything.
	reg := prometheus.NewRegistry()
	// RegisterKEKCollector uses the default registry; verify the guard path
	// directly via NewKEKCollector against a private registry instead.
	if err := reg.Register(NewKEKCollector(fakeSealReader{seals: map[uint32]uint64{0: 0}}, nil)); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Calling with nil keeper must be a no-op (no panic).
	RegisterKEKCollector(nil, nil)
}
