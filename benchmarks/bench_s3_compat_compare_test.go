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
