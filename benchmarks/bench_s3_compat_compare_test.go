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

func TestBenchS3CompatDoesNotPublishErroredWarpRows(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	for _, want := range []string{
		`if errors > 0:`,
		`sys.exit(f"non-zero warp errors: {errors}")`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_s3_compat_compare.sh must reject errored warp analyze rows with %q", want)
		}
	}
}

func TestBenchS3CompatExitsNonZeroForUnpublishableWarpResults(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	for _, want := range []string{
		`RUN_FAILURES=0`,
		`RUN_FAILURES=1`,
		`if (( RUN_FAILURES != 0 )); then`,
		`exit 1`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_s3_compat_compare.sh must propagate warp measurement failures with %q", want)
		}
	}
}

func TestBenchS3CompatCanFailFastOnDirtyHost(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	common, err := os.ReadFile("lib/common.sh")
	if err != nil {
		t.Fatal(err)
	}
	commonScript := string(common)

	for _, want := range []string{
		`bench_collect_host_preflight "$PROFILE_ROOT"`,
		`bench_enforce_strict_host "$PROFILE_ROOT"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_s3_compat_compare.sh must support strict dirty-host preflight with %q", want)
		}
	}
	for _, want := range []string{
		`BENCH_STRICT_HOST`,
		`BENCH_MAX_LOAD_PER_CPU`,
		`BENCH_ALLOW_EXTERNAL_GRAINFS`,
		`BENCH_HOST_LOAD_PER_CPU`,
		`BENCH_HOST_PREFLIGHT_FAILURES=1`,
		`BENCH_HOST_GRAINFS_ALLOWED=1`,
		`float(load_per_cpu) > float(max_load_per_cpu)`,
		`if [[ "$BENCH_STRICT_HOST" == "1" && "${BENCH_HOST_PREFLIGHT_FAILURES:-0}" == "1" ]]; then`,
		`exit 1`,
	} {
		if !strings.Contains(commonScript, want) {
			t.Fatalf("common.sh must support strict dirty-host preflight with %q", want)
		}
	}
}

func TestHostPreflightDoesNotCountItsOwnProcessScan(t *testing.T) {
	common, err := os.ReadFile("lib/common.sh")
	if err != nil {
		t.Fatal(err)
	}
	commonScript := string(common)

	if strings.Contains(commonScript, `awk '/grainfs serve/`) {
		t.Fatalf("host preflight process scan can match its own awk command")
	}
	if !strings.Contains(commonScript, `awk '/[g]rainfs serve/`) {
		t.Fatalf("host preflight process scan must avoid matching its own awk command")
	}
}

func TestBenchS3CompatCleanupOnlyAnnouncesWhenBackendsStarted(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	for _, want := range []string{
		`BACKENDS_STARTED=0`,
		`BACKENDS_STARTED=1`,
		`if [[ "$BACKENDS_STARTED" == "1" ]]; then`,
		`[bench] stopping comparison backends`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_s3_compat_compare.sh cleanup should only announce started backends with %q", want)
		}
	}
}

func TestBenchS3CompatUsesUniqueDefaultBenchDir(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	if strings.Contains(script, `BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-s3-compat-compare}"`) {
		t.Fatalf("default BENCH_DIR must not reuse and pre-delete a fixed /tmp path")
	}
	for _, want := range []string{
		`BENCH_DIR_PROVIDED=0`,
		`BENCH_DIR="$(mktemp -d "${TMPDIR:-/tmp}/grainfs-s3-compat-compare.XXXXXX")"`,
		`if [[ "$BENCH_DIR_PROVIDED" == "1" ]]; then`,
		`rm -rf "$BENCH_DIR"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_s3_compat_compare.sh must use unique default BENCH_DIR while preserving explicit BENCH_DIR cleanup with %q", want)
		}
	}
}

func TestIcebergBenchesUseHostPreflight(t *testing.T) {
	for _, path := range []string{"bench_iceberg_table.sh", "bench_iceberg_table_cluster.sh"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		script := string(body)
		for _, want := range []string{
			`bench_collect_host_preflight "$PROFILE_DIR"`,
			`bench_enforce_strict_host "$PROFILE_DIR"`,
			`host-preflight.txt`,
		} {
			if !strings.Contains(script, want) {
				t.Fatalf("%s must use host preflight with %q", path, want)
			}
		}
	}
}

func TestBenchS3CompatDoesNotAcceptArbitraryServeFlags(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	for _, forbidden := range []string{
		`EXTRA_GRAINFS_SERVE_FLAGS`,
		`extra_flags`,
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("bench_s3_compat_compare.sh must not accept arbitrary serve flags via %q", forbidden)
		}
	}
}

func TestBenchBootstrapIAMSeedsTrustedProxyCIDR(t *testing.T) {
	body, err := os.ReadFile("lib/common.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	for _, want := range []string{
		`/v1/config/trusted-proxy.cidr`,
		`{"value":"127.0.0.1/32"}`,
		`curl -sf --unix-socket "$admin_sock"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_bootstrap_iam_credentials must seed trusted proxy CIDR with %q", want)
		}
	}
}

func TestBenchS3CompatClusterStagesKEKBeforeJoinersStart(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	start := strings.Index(script, "start_grainfs_cluster()")
	if start < 0 {
		t.Fatal("start_grainfs_cluster not found")
	}
	end := strings.Index(script[start:], "start_minio()")
	if end < 0 {
		t.Fatal("start_minio not found")
	}
	cluster := script[start : start+end]

	if !strings.Contains(cluster, `local cluster_dir="$BENCH_DIR/gfc"`) {
		t.Fatalf("start_grainfs_cluster must keep data dir names short so admin.sock does not exceed Unix socket path limits")
	}
	for _, want := range []string{
		`local kek_file="$cluster_dir/n1/keys/0.key"`,
		`local cluster_id_file="$cluster_dir/n1/cluster.id"`,
		`bench_wait_file "$kek_file" "grainfs-cluster node1 KEK"`,
		`bench_wait_file "$cluster_id_file" "grainfs-cluster node1 cluster.id"`,
		`mkdir -p "$cluster_dir/n${idx}/keys"`,
		`cp "$kek_file" "$cluster_dir/n${idx}/keys/0.key"`,
		`chmod 600 "$cluster_dir/n${idx}/keys/0.key"`,
		`cp "$cluster_id_file" "$cluster_dir/n${idx}/cluster.id"`,
		`chmod 600 "$cluster_dir/n${idx}/cluster.id"`,
	} {
		if !strings.Contains(cluster, want) {
			t.Fatalf("start_grainfs_cluster must stage KEK + cluster.id for joiners with %q", want)
		}
	}
	if strings.Index(cluster, `cp "$kek_file" "$cluster_dir/n${idx}/keys/0.key"`) > strings.Index(cluster, `start_grainfs_cluster_node "$idx"`) {
		t.Fatalf("start_grainfs_cluster must copy KEK before starting joiner nodes")
	}
}

func TestBenchS3CompatWaitsForGrainFSBucketAuthOnEveryClusterNode(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	common, err := os.ReadFile("lib/common.sh")
	if err != nil {
		t.Fatal(err)
	}
	commonScript := string(common)

	if !strings.Contains(commonScript, "bench_wait_s3_bucket_auth_ready()") {
		t.Fatalf("common.sh must define bench_wait_s3_bucket_auth_ready")
	}
	if !strings.Contains(commonScript, `AWS_ACCESS_KEY_ID="$access_key"`) {
		t.Fatalf("bench_wait_s3_bucket_auth_ready must use the benchmark access key")
	}
	if !strings.Contains(commonScript, `--endpoint-url "$url" s3api head-bucket --bucket "$bucket"`) {
		t.Fatalf("bench_wait_s3_bucket_auth_ready must probe each endpoint with signed HeadBucket")
	}

	prepare := strings.Index(script, `prepare_grainfs_warp_bucket "$target" "$op"`)
	waitReady := strings.Index(script, `bench_wait_s3_bucket_auth_ready "$base_url" "$access_key" "$secret_key" "warp-${target}-${op}"`)
	run := strings.Index(script, `run_warp_case "$target" "$base_url" "$access_key" "$secret_key" "$op"`)
	if waitReady < 0 {
		t.Fatalf("bench_s3_compat_compare.sh must wait for GrainFS bucket auth readiness before warp")
	}
	if !(prepare < waitReady && waitReady < run) {
		t.Fatalf("bucket auth readiness wait must run after bucket preparation and before warp")
	}
}

func TestBenchS3CompatWaitsForMinIOClusterSignedWriteReadiness(t *testing.T) {
	body, err := os.ReadFile("bench_s3_compat_compare.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	common, err := os.ReadFile("lib/common.sh")
	if err != nil {
		t.Fatal(err)
	}
	commonScript := string(common)

	if !strings.Contains(commonScript, "bench_wait_s3_signed_write_ready()") {
		t.Fatalf("common.sh must define bench_wait_s3_signed_write_ready")
	}
	if !strings.Contains(commonScript, `s3api create-bucket --bucket "$bucket"`) {
		t.Fatalf("signed write readiness must create a probe bucket")
	}
	if !strings.Contains(commonScript, `s3api put-object --bucket "$bucket" --key "$key" --body "$body_file"`) {
		t.Fatalf("signed write readiness must PUT through each endpoint")
	}

	start := strings.Index(script, "start_minio_cluster()")
	if start < 0 {
		t.Fatal("start_minio_cluster not found")
	}
	end := strings.Index(script[start:], "start_rustfs()")
	if end < 0 {
		t.Fatal("start_rustfs not found")
	}
	minioCluster := script[start : start+end]

	healthReady := strings.Index(minioCluster, `echo "  minio-cluster S3 cluster-ready"`)
	writeReady := strings.Index(minioCluster, `bench_wait_s3_signed_write_ready "$(IFS=','; echo "${urls[*]}")" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "warp-minio-cluster-ready"`)
	setStart := strings.Index(minioCluster, `set_start_info "$(IFS=','; echo "${urls[*]}")" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "local"`)
	if writeReady < 0 {
		t.Fatalf("start_minio_cluster must wait for signed write readiness before publishing endpoints")
	}
	if !(healthReady < writeReady && writeReady < setStart) {
		t.Fatalf("minio signed write readiness must run after health readiness and before set_start_info")
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

func TestIcebergClusterBenchCopiesKEKBeforeJoinersStart(t *testing.T) {
	body, err := os.ReadFile("bench_iceberg_table_cluster.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	copyKEK := strings.Index(script, `cp "$BENCH_DIR/n0/keys/0.key" "$BENCH_DIR/n$i/keys/0.key"`)
	copyClusterID := strings.Index(script, `cp "$BENCH_DIR/n0/cluster.id" "$BENCH_DIR/n$i/cluster.id"`)
	joinPending := strings.Index(script, `printf '%s' "$(raft_addr 0)" >"$BENCH_DIR/n$i/.join-pending"`)
	startJoiner := strings.Index(script, `start_node "$i"`)
	if copyKEK < 0 {
		t.Fatalf("bench_iceberg_table_cluster.sh must copy n0 keys/0.key before starting joiners")
	}
	if copyClusterID < 0 {
		t.Fatalf("bench_iceberg_table_cluster.sh must copy n0 cluster.id before starting joiners")
	}
	if !(copyKEK < joinPending && copyClusterID < joinPending && joinPending < startJoiner) {
		t.Fatalf("Iceberg cluster joiner KEK + cluster.id copy must happen before join-pending and start_node")
	}
}
