package benchmarks

import (
	"os"
	"strings"
	"testing"
)

func TestBenchGoAPIMicroCleansScratchDirAndKeepsProfilesUnderProfileRoot(t *testing.T) {
	body, err := os.ReadFile("bench_go_api_micro.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	for _, want := range []string{
		`RUN_TMP="$(mktemp -d "${bench_tmp_base%/}/grainfs-go-api-micro.XXXXXX")"`,
		`CLEANED_UP=0`,
		`rm -rf "$RUN_TMP"`,
		`trap cleanup EXIT`,
		`trap 'cleanup; exit 130' INT`,
		`trap 'cleanup; exit 143' TERM`,
		`PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/go-api-micro-$(date +%Y%m%d-%H%M%S)}"`,
		`-cpuprofile "$out_dir/cpu.pb.gz"`,
		`-memprofile "$out_dir/mem.pb.gz"`,
		`GRAINFS_S3_BENCH_DRIVES=4`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bench_go_api_micro.sh must contain %q", want)
		}
	}
}
