package server

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// goldenFixtureCluster is a deterministic ClusterInfo for wire golden tests.
// 3 voters: n1 (self/leader), n2 (live), n3 (live cooldown peer for variety).
type goldenFixtureCluster struct{}

func (goldenFixtureCluster) NodeID() string      { return "n1" }
func (goldenFixtureCluster) State() string       { return "Leader" }
func (goldenFixtureCluster) Term() uint64        { return 7 }
func (goldenFixtureCluster) LeaderID() string    { return "n1" }
func (goldenFixtureCluster) Peers() []string     { return []string{"n2", "n3"} }
func (goldenFixtureCluster) LivePeers() []string { return []string{"n1", "n2", "n3"} }

func (goldenFixtureCluster) Snapshot() cluster.ClusterStatus {
	return cluster.ClusterStatus{
		PeerSnapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
			{PeerID: "n2", RaftAddr: "10.0.0.2:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
			{PeerID: "n3", RaftAddr: "10.0.0.3:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
		},
		BucketAssignments: map[string]string{"b": "g1"},
		ShardGroups: []cluster.ShardGroupEntry{
			{ID: "g1", PeerIDs: []string{"n1", "n2", "n3"}},
		},
	}
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
// admin endpoints under test wired to deterministic fixtures. Returns an
// http.Client that dials the UDS and a base URL "http://unix" the caller
// appends paths to.
func newGoldenFixtureServer(t *testing.T) (*http.Client, string) {
	t.Helper()

	d, err := os.MkdirTemp("/tmp", "gs-golden-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "a.sock")

	srv := &Server{
		cluster:  goldenFixtureCluster{},
		balancer: goldenFixtureBalancer{},
	}

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
	)
	srv.RegisterClusterAdminUDS(h)

	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.Dial("unix", sock)
		if dialErr == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cli := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 5 * time.Second,
	}
	return cli, "http://unix"
}

// updateGolden rewrites testdata/*.golden.json with the current handler
// output. Run `go test -update-golden ...` after intentional wire changes.
var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/*.golden.json")

// assertGoldenJSON compares the raw response body (JSON) against the file at
// testdata/<name>. With -update-golden it rewrites the file instead.
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
		if err := os.WriteFile(path, indented.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with -update-golden to create)", path, err)
	}
	if !bytes.Equal(indented.Bytes(), want) {
		t.Fatalf("wire diff for %s:\n--- got\n%s\n--- want\n%s",
			name, indented.String(), string(want))
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
