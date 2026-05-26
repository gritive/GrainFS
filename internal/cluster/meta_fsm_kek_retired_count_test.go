package cluster

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestRetiredKEKVersionCount verifies the FSM-lifecycle retired count: only
// versions whose status is retiring or pruned are counted, the active version
// is never counted, and a version with no status entry (post-rotation, no
// operator retire) is implicitly active and excluded.
func TestRetiredKEKVersionCount(t *testing.T) {
	kek := func(b byte) []byte { return bytes.Repeat([]byte{b}, encrypt.KEKSize) }

	tests := []struct {
		name     string
		versions []uint32
		active   uint32
		statuses map[uint32]KEKLifecycleStatus
		want     int
	}{
		{
			name:     "post-rotation no retire → 0 (regression guard)",
			versions: []uint32{0, 1},
			active:   1,
			statuses: nil, // V0 has no status entry → implicitly active
			want:     0,
		},
		{
			name:     "one retiring",
			versions: []uint32{0, 1, 2},
			active:   2,
			statuses: map[uint32]KEKLifecycleStatus{1: KEKLifecycleRetiring},
			want:     1,
		},
		{
			name:     "retiring plus pruned",
			versions: []uint32{0, 1, 2},
			active:   2,
			statuses: map[uint32]KEKLifecycleStatus{0: KEKLifecyclePruned, 1: KEKLifecycleRetiring},
			want:     2,
		},
		{
			name:     "active never counted even with stale status",
			versions: []uint32{0, 1},
			active:   1,
			statuses: map[uint32]KEKLifecycleStatus{1: KEKLifecycleRetiring},
			want:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewMetaFSM()
			store := encrypt.NewKEKStore()
			for _, v := range tc.versions {
				if err := store.Add(v, kek(byte(v))); err != nil {
					t.Fatalf("seed KEKStore v=%d: %v", v, err)
				}
			}
			fsm.SetKEKStore(store)
			fsm.SetActiveKEKVersion(tc.active)
			for v, s := range tc.statuses {
				fsm.SetKEKStatus(v, s, 1)
			}
			if got := fsm.RetiredKEKVersionCount(); got != tc.want {
				t.Errorf("RetiredKEKVersionCount = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestRetiredKEKVersionCount_NoStore returns 0 when no keystore is wired.
func TestRetiredKEKVersionCount_NoStore(t *testing.T) {
	fsm := NewMetaFSM()
	if got := fsm.RetiredKEKVersionCount(); got != 0 {
		t.Errorf("RetiredKEKVersionCount with no store = %d, want 0", got)
	}
}
