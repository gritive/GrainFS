package cluster

import "testing"

// TestReclaimCertainty pins the decision table for the DESTRUCTIVE orphan-reclaim
// read. found and certain are INDEPENDENT: a reclaim makes a NEGATIVE claim
// ("this object/version is absent"), valid only against a COMPLETE responding set,
// so ANY errored/unreachable peer forces certain=false even if another peer
// returned a manifest. Callers reclaim ONLY when certain && !found.
func TestReclaimCertainty(t *testing.T) {
	const D, N, E = peerHasData, peerNotFound, peerErrored
	cases := []struct {
		name           string
		localHasData   bool
		localUncertain bool
		peers          []peerReadOutcome
		wantFound      bool
		wantCertain    bool
	}{
		{"local read error → uncertain", false, true, nil, false, false},
		{"solo: local has data → found, certain", true, false, nil, true, true},
		{"solo: local absent → certain orphan", false, false, nil, false, true},
		{"cluster: local has data, peers clean → found, certain", true, false, []peerReadOutcome{N}, true, true},
		{"cluster: local has data BUT a peer errored → found, NOT certain", true, false, []peerReadOutcome{E}, true, false},
		{"cluster: absent, all peers not-found → certain orphan", false, false, []peerReadOutcome{N, N}, false, true},
		{"cluster: absent, a peer has data → found, certain", false, false, []peerReadOutcome{N, D}, true, true},
		{"cluster: absent, a peer errored → uncertain (KEEP)", false, false, []peerReadOutcome{N, E}, false, false},
		{"cluster: absent, all peers errored → uncertain", false, false, []peerReadOutcome{E, E}, false, false},
		{"cluster: a peer has data AND another errored → found, NOT certain", false, false, []peerReadOutcome{D, E}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			found, certain := reclaimCertainty(tc.localHasData, tc.localUncertain, tc.peers)
			if found != tc.wantFound || certain != tc.wantCertain {
				t.Fatalf("reclaimCertainty(localHasData=%v, localUncertain=%v, %v) = (found=%v, certain=%v); want (found=%v, certain=%v)",
					tc.localHasData, tc.localUncertain, tc.peers, found, certain, tc.wantFound, tc.wantCertain)
			}
		})
	}
}
