package server

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

// goldenFixtureCluster is a deterministic ClusterInfo for wire golden tests.
// Embeds *fakeClusterInfo for the base methods; overrides Snapshot() to add
// BucketAssignments + ShardGroups (matching fakeTopologyClusterInfo's pattern)
// and ObjectIndexSummary/PlacementReport for placement endpoint coverage.
type goldenFixtureCluster struct {
	*fakeClusterInfo
}

func (g goldenFixtureCluster) Snapshot() cluster.ClusterStatus {
	snap := g.fakeClusterInfo.Snapshot()
	snap.BucketAssignments = map[string]string{"b": "g1"}
	snap.ShardGroups = []cluster.ShardGroupEntry{
		{ID: "g1", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	return snap
}

func (goldenFixtureCluster) ObjectIndexSummary(bucket string) cluster.ObjectIndexSummary {
	return cluster.ObjectIndexSummary{
		Bucket:               bucket,
		PlacementGroupCounts: map[string]int{"g1": 3},
	}
}

func (goldenFixtureCluster) PlacementReport(bucket, key string, maxRows int) cluster.PlacementReport {
	return cluster.PlacementReport{
		DesiredPolicyBasis:  "group_voter_count",
		Bucket:              bucket,
		Key:                 key,
		ObjectCount:         1,
		Bytes:               1024,
		ActualProfileCounts: map[string]int{"4+2": 1},
		Details: []cluster.PlacementReportEntry{
			{
				Bucket:           "b",
				Key:              "obj-1",
				VersionID:        "v1",
				PlacementGroupID: "g1",
				ActualECData:     4,
				ActualECParity:   2,
				DesiredECData:    4,
				DesiredECParity:  2,
				LayoutState:      cluster.LayoutCurrent,
				NodeIDs:          []string{"n1", "n2", "n3"},
				Size:             1024,
			},
		},
	}
}

// goldenFixtureBalancer is a deterministic BalancerInfo for wire golden tests.
// Mix of nodes with non-zero DiskUsedPct and one with zero JoinedAt — exercises
// the omitempty behavior on JoinedAt that Task 9 changes the producer for.
type goldenFixtureBalancer struct{}

// fixedJoinedAt / fixedUpdatedAt produce deterministic JSON. UTC + non-zero
// nanos kept stable so byte equality holds across runs.
var (
	fixedJoinedAt  = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fixedUpdatedAt = time.Date(2026, 1, 2, 4, 5, 6, 0, time.UTC)
)

func (goldenFixtureBalancer) Status() BalancerStatusResult {
	return BalancerStatusResult{
		Active:       true,
		ImbalancePct: 12.5,
		Nodes: []BalancerNodeInfo{
			{
				NodeID:         "n1",
				DiskUsedPct:    55.0,
				DiskAvailBytes: 100 << 30,
				RequestsPerSec: 10.0,
				JoinedAt:       fixedJoinedAt,
				UpdatedAt:      fixedUpdatedAt,
			},
			{
				NodeID:         "n2",
				DiskUsedPct:    72.5,
				DiskAvailBytes: 60 << 30,
				RequestsPerSec: 4.5,
				JoinedAt:       fixedJoinedAt,
				UpdatedAt:      fixedUpdatedAt,
			},
			{
				NodeID:         "n3",
				DiskUsedPct:    0,
				DiskAvailBytes: 200 << 30,
				RequestsPerSec: 0,
				// JoinedAt zero — exercises omitempty.
				UpdatedAt: fixedUpdatedAt,
			},
		},
	}
}

// newGoldenFixtureServer brings up a Hertz instance on a UDS with the four
// admin endpoints under test wired to deterministic fixtures. Reuses the
// shared UDS helpers from cluster_admin_uds_test.go. Returns an http.Client
// that dials the UDS and a base URL "http://unix" the caller appends paths to.
func newGoldenFixtureServer(t *testing.T) (*http.Client, string) {
	t.Helper()

	srv := &Server{
		cluster: goldenFixtureCluster{
			fakeClusterInfo: &fakeClusterInfo{
				nodeID:    "n1",
				state:     "Leader",
				term:      7,
				leaderID:  "n1",
				peers:     []string{"n2", "n3"},
				livePeers: []string{"n1", "n2", "n3"},
				snapshot: []cluster.PeerLivenessRow{
					{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
					{PeerID: "n2", RaftAddr: "10.0.0.2:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
					{PeerID: "n3", RaftAddr: "10.0.0.3:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
				},
			},
		},
		balancer: goldenFixtureBalancer{},
	}

	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, srv)
	return cli, "http://unix"
}

// updateGolden rewrites testdata/*.golden.json with the current handler
// output. Run `go test -update-golden ...` after intentional wire changes.
var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/*.golden.json")

// assertGoldenJSON compares the raw response body (JSON) against the file at
// testdata/<name>. With -update-golden it rewrites the file instead. Goldens
// are written with a trailing newline; comparison trims a single trailing
// newline from both sides so editor-added newlines don't cause spurious diffs.
func assertGoldenJSON(t *testing.T, name string, raw []byte) {
	t.Helper()

	var indented bytes.Buffer
	if err := json.Indent(&indented, raw, "", "  "); err != nil {
		t.Fatalf("response is not valid JSON: %v\nbody: %s", err, raw)
	}

	path := filepath.Join("testdata", name)
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		out := append(indented.Bytes(), '\n')
		if err := os.WriteFile(path, out, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with -update-golden to create)", path, err)
	}
	got := bytes.TrimSuffix(indented.Bytes(), []byte("\n"))
	wantTrim := bytes.TrimSuffix(want, []byte("\n"))
	if !bytes.Equal(got, wantTrim) {
		t.Fatalf("wire diff for %s:\n--- got\n%s\n--- want\n%s",
			name, string(got), string(wantTrim))
	}
}

// fetchBody GETs path on the fixture client and returns the raw response body.
func fetchBody(t *testing.T, cli *http.Client, url string) []byte {
	t.Helper()
	resp, err := cli.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
