package benchmarks

import (
	"os"
	"strings"
	"testing"
)

func TestBenchS3CompatPrecreatesLocalGrainFSWarpBuckets(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	if !strings.Contains(script, "prepare_grainfs_warp_bucket()") {
		t.Fatalf("bench_s3_compat_compare.sh must define prepare_grainfs_warp_bucket")
	}
	if !strings.Contains(script, `bench_create_bucket_with_policy_admin_retry "$BINARY" "$GRAINFS_ADMIN_DATA_DIR" "warp-${target}-${op}" "$GRAINFS_SA_ID" bucket-admin`) {
		t.Fatalf("prepare_grainfs_warp_bucket must create warp's per-op bucket with bucket-admin attached to the benchmark SA")
	}

	prepare := strings.Index(script, `prepare_grainfs_warp_bucket "$target" "$op"`)
	run := strings.Index(script, `run_warp_case "$target" "$base_url" "$access_key" "$secret_key" "$op"`)
	if prepare < 0 {
		t.Fatalf("bench_s3_compat_compare.sh must call prepare_grainfs_warp_bucket before each warp run")
	}
	if run < 0 {
		t.Fatalf("bench_s3_compat_compare.sh must still call run_warp_case")
	}
	if prepare > run {
		t.Fatalf("prepare_grainfs_warp_bucket must run before run_warp_case")
	}
}

func TestBenchS3CompatRecordsResourceSkew(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	if !strings.Contains(script, "collect_resource_snapshot()") {
		t.Fatalf("bench_s3_compat_compare.sh must define collect_resource_snapshot")
	}
	if !strings.Contains(script, `"$PROFILE_ROOT/resource-results.tsv"`) {
		t.Fatalf("bench_s3_compat_compare.sh must write resource-results.tsv")
	}
	if !strings.Contains(script, "append_resource_summary") {
		t.Fatalf("bench_s3_compat_compare.sh must append resource skew summary rows")
	}
	if !strings.Contains(script, `collect_resource_snapshot "$target" "$op" "$target_pid_start"`) {
		t.Fatalf("bench_s3_compat_compare.sh must collect a resource snapshot after each warp op")
	}
}

func TestBenchS3CompatCanProfileSingleNodeGrainFS(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	for _, want := range []string{
		`GRAINFS_PPROF_PORTS=("$PPROF_BASE_PORT")`,
		`extra+=(--pprof-port "$PPROF_BASE_PORT")`,
		`bench_wait_tcp_port "127.0.0.1" "$PPROF_BASE_PORT" "grainfs-single pprof"`,
		`"$target" == grainfs-*`,
		`"$PROFILE_ROOT/$target/pprof-snap/node$((i+1))"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_s3_compat_compare.sh must contain %q", want)
		}
	}
}

func TestBenchS3CompatSingleNodeAcceptsExtraServeFlags(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	start := strings.Index(script, "start_grainfs_single()")
	if start < 0 {
		t.Fatal("start_grainfs_single not found")
	}
	end := strings.Index(script[start:], "start_grainfs_cluster()")
	if end < 0 {
		t.Fatal("start_grainfs_cluster not found")
	}
	single := script[start : start+end]

	for _, want := range []string{
		`local extra_flags=()`,
		`read -r -a extra_flags <<<"$EXTRA_GRAINFS_SERVE_FLAGS"`,
		`"${extra_flags[@]}"`,
	} {
		if !strings.Contains(single, want) {
			t.Fatalf("start_grainfs_single must contain %q", want)
		}
	}
}

func TestIcebergClusterBenchCreatesWarehouseBucketWithPolicy(t *testing.T) {
	body, err := os.ReadFile("bench_iceberg_table_cluster.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	if !strings.Contains(script, `bench_create_bucket_with_policy_admin_retry "$BINARY" "$BENCH_DIR/n$TARGET_INDEX" "$ICEBERG_BUCKET" "$SA_ID" bucket-admin`) {
		t.Fatalf("bench_iceberg_table_cluster.sh must create the warehouse bucket with bucket-admin attached to the benchmark SA")
	}
}
