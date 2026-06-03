package benchmarks

import (
	"os"
	"strings"
	"testing"
)

// TestBenchIcebergClusterJoinsViaInviteBundle guards the iceberg cluster harness
// against the dead `.join-pending` follower-join model. That model copied the seed
// KEK + cluster.id into every node and wrote a `.join-pending` file that boot never
// reads (since the Zero-CA invite-join model landed), so nodes 1..N came up as
// isolated solo clusters. The SA bootstrapped on node 0 was then unknown to the
// others, and `warp iceberg` failed at catalog-pool creation with
// "NotAuthorizedException: unknown access key". The harness must instead join nodes
// via single-use invite bundles minted on the seed's admin socket.
func TestBenchIcebergClusterJoinsViaInviteBundle(t *testing.T) {
	body, err := os.ReadFile("bench_iceberg_table_cluster.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	for _, want := range []string{
		// Seed stages only its own transport PSK; joiners do not.
		`printf '%s\n' "bench-iceberg-cluster-key" >"$BENCH_DIR/n$i/keys.d/current.key"`,
		// Every node serves/dials a Zero-CA join listener.
		`--join-listen-addr "$(join_addr "$i")"`,
		// Each joiner is minted a single-use bundle on the seed (node 0) admin socket.
		`cluster invite create --endpoint "$BENCH_DIR/n0/admin.sock"`,
		// Joiners boot with the bundle in the environment, not with copied keys.
		`env "GRAINFS_INVITE_BUNDLE=$invite_bundle"`,
		`start_node "$i" "$bundle"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("iceberg cluster harness must join nodes via the invite-bundle flow; missing %q", want)
		}
	}

	// The legacy dead-code join must not return as LIVE script (a comment that
	// documents its removal is fine, so check for the actual write + KEK copy).
	for _, banned := range []string{
		`>"$BENCH_DIR/n$i/.join-pending"`,
		`cp "$BENCH_DIR/n0/keys/0.key" "$BENCH_DIR/n$i/keys/0.key"`,
	} {
		if strings.Contains(script, banned) {
			t.Fatalf("iceberg cluster harness must NOT use the dead .join-pending/KEK-copy join; found %q", banned)
		}
	}
}
