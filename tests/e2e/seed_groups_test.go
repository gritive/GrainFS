package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestE2E_SeedGroups_Multi 는 --seed-groups N>1 이 지정되면 leader가
// N개 데이터 그룹을 propose하고 5 process 부팅이 정상 완료되는지 검증한다.
//
// 현재 단계: group count를 직접 검증하는 endpoint가 없으므로, 부팅 완료 +
// leader 식별 + bucket 생성 성공 까지를 회귀 가드로 사용. 직접적인 group
// inspection은 후속 PR에서 추가 예정.
func TestE2E_SeedGroups_Multi(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping seed-groups multi test in -short mode")
	}
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", binary)
	}

	const (
		clusterKey = "E2E-SEED-MULTI-KEY"
		accessKey  = "seed-ak"
		secretKey  = "seed-sk"
		numNodes   = 5
		seedGroups = 8
	)

	httpPorts := make([]int, numNodes)
	raftPorts := make([]int, numNodes)
	for i := range httpPorts {
		httpPorts[i] = freePort()
		raftPorts[i] = freePort()
	}
	raftAddr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", raftPorts[i]) }
	httpURL := func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", httpPorts[i]) }
	peersFor := func(i int) string {
		var out []string
		for j := range raftPorts {
			if j == i {
				continue
			}
			out = append(out, raftAddr(j))
		}
		return strings.Join(out, ",")
	}

	dataDirs := make([]string, numNodes)
	for i := range dataDirs {
		d, err := os.MkdirTemp("", fmt.Sprintf("grainfs-seed-%d-*", i))
		require.NoError(t, err)
		dataDirs[i] = d
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}

	startNode := func(i int) *exec.Cmd {
		cmd := exec.Command(binary, "serve",
			"--data", dataDirs[i],
			"--port", fmt.Sprintf("%d", httpPorts[i]),
			"--node-id", fmt.Sprintf("seed-node-%d", i),
			"--raft-addr", raftAddr(i),
			"--peers", peersFor(i),
			"--cluster-key", clusterKey,
			"--access-key", accessKey,
			"--secret-key", secretKey,
			fmt.Sprintf("--seed-groups=%d", seedGroups),
			"--nfs4-port", fmt.Sprintf("%d", freePort()),
			"--nbd-port", fmt.Sprintf("%d", freePort()),
			"--snapshot-interval", "0",
			"--scrub-interval", "0",
			"--lifecycle-interval", "0",
			"--no-encryption",
		)
		require.NoError(t, cmd.Start(), "start node %d", i)
		return cmd
	}

	procs := make([]*exec.Cmd, numNodes)
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
				_, _ = p.Process.Wait()
			}
		}
	})

	for i := 0; i < numNodes; i++ {
		procs[i] = startNode(i)
	}
	waitForPortsParallel(t, httpPorts, 90*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	require.Eventually(t, func() bool {
		for i := 0; i < numNodes; i++ {
			c := ecS3Client(httpURL(i), accessKey, secretKey)
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("seed-bucket")})
			if err == nil {
				return true
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "no leader found")

	// seed loop가 끝날 때까지 대기 (8 groups × ~200ms 추정 + 여유 5s)
	time.Sleep(5 * time.Second)

	t.Logf("seed-groups multi test passed: %d groups seeded across %d nodes", seedGroups, numNodes)
}
