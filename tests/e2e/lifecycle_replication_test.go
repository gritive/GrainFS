package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
)

const lifecycleXML = `<LifecycleConfiguration><Rule><ID>r1</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`

// lifecycleSigner는 테스트 전반에서 SigV4 서명 헬퍼를 제공한다.
type lifecycleSigner struct {
	signer *v4.Signer
	creds  aws.Credentials
}

func newLifecycleSigner(accessKey, secretKey string) lifecycleSigner {
	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
	})
	return lifecycleSigner{
		signer: signer,
		creds:  aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey},
	}
}

// signedLifecyclePut는 SigV4로 서명된 PUT /<bucket>?lifecycle 요청을 발송하고 상태 코드를 반환한다.
func (ls lifecycleSigner) signedLifecyclePut(t *testing.T, baseURL, bucket string) int {
	t.Helper()
	ctx := context.Background()
	body := []byte(lifecycleXML)
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])

	url := baseURL + "/" + bucket + "?lifecycle"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = ls.signer.SignHTTP(ctx, ls.creds, req, payloadHash, "s3", "us-east-1", time.Now())
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Logf("PUT %s?lifecycle -> %d: %s", bucket, resp.StatusCode, string(respBody))
	}
	return resp.StatusCode
}

// signedLifecycleGet는 SigV4로 서명된 GET /<bucket>?lifecycle 요청을 발송하고 상태 코드를 반환한다.
func (ls lifecycleSigner) signedLifecycleGet(t *testing.T, baseURL, bucket string) int {
	t.Helper()
	ctx := context.Background()
	sum := sha256.Sum256(nil)
	payloadHash := hex.EncodeToString(sum[:])

	url := baseURL + "/" + bucket + "?lifecycle"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = ls.signer.SignHTTP(ctx, ls.creds, req, payloadHash, "s3", "us-east-1", time.Now())
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

// waitClusterSettled는 3-voter 클러스터가 안정될 때까지 기다린다.
// getStatusJSON이 "peers" 2개를 보고할 때 완전히 합류한 것으로 간주.
func waitClusterSettled(t *testing.T, leaderURL string) {
	t.Helper()
	require.Eventually(t, func() bool {
		s := getStatusJSON(t, leaderURL)
		return len(stringList(s["peers"])) == 2
	}, 90*time.Second, 500*time.Millisecond, "3-node cluster must settle (peers==2)")
}

// TestLifecycleE2E collapses lifecycle replication checks (follower→leader
// PUT forward, leader-change preserves replicated config) into one entry.
// Both sub-tests need a 3-voter DynamicJoin cluster — the second one kills
// the leader, so it runs last in code order; the first sub-test runs
// against the pristine cluster.
func TestLifecycleE2E(t *testing.T) {
	t.Run("Cluster3Node", func(t *testing.T) {
		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:      3,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-LIFECYCLE-REPL-KEY",
			LogPrefix:  "grainfs-lifecycle",
			ExtraArgs:  []string{"--lifecycle-interval=24h"},
		})
		runLifecycleCases(t, c)
	})
}

func runLifecycleCases(t *testing.T, c *e2eCluster) {
	t.Helper()
	leaderIdx := c.leaderIdx
	require.GreaterOrEqual(t, leaderIdx, 0, "harness must have identified leader")
	waitClusterSettled(t, c.httpURLs[leaderIdx])

	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 15*time.Second)
	}

	ls := newLifecycleSigner(c.accessKey, c.secretKey)

	t.Run("FollowerPutLeaderGet", func(t *testing.T) {
		followerIdx := -1
		for i := range c.procs {
			if i != leaderIdx {
				followerIdx = i
				break
			}
		}
		require.GreaterOrEqual(t, followerIdx, 0, "must find a follower node")

		const bucket = "lc-follower-put"
		c.GrantAdminOnBuckets(bucket)
		createBucketWithClient(t, c.S3Client(leaderIdx), bucket)

		status := ls.signedLifecyclePut(t, c.httpURLs[followerIdx], bucket)
		require.Equal(t, http.StatusOK, status,
			"follower PUT /%s?lifecycle must return 200 (ADR 0011: follower must forward to leader)", bucket)

		require.Eventually(t, func() bool {
			return ls.signedLifecycleGet(t, c.httpURLs[leaderIdx], bucket) == http.StatusOK
		}, 5*time.Second, 100*time.Millisecond,
			"leader must observe replicated lifecycle config within 5s")
	})

	t.Run("LeaderChangePreservesConfig", func(t *testing.T) {
		// destructive: kills the current leader to force re-election.
		// must run last in code order — subsequent sub-tests would see a
		// post-leader-kill cluster.
		runLifecycleLeaderChangeSubtest(t, c, ls)
	})
}

func runLifecycleLeaderChangeSubtest(t *testing.T, c *e2eCluster, ls lifecycleSigner) {
	t.Helper()
	leaderIdx := c.leaderIdx
	s := getStatusJSON(t, c.httpURLs[leaderIdx])
	initialLeader, _ := s["leader_id"].(string)
	require.NotEmpty(t, initialLeader, "must resolve current leader id from status")

	// leader_id → 노드 인덱스 매핑
	currentLeaderIdx := -1
	for i := range c.procs {
		if c.nodeID(i) == initialLeader {
			currentLeaderIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, currentLeaderIdx, 0, "must locate current leader by node id")

	// 새 리더 후보가 IAM 키를 이미 받았는지 재확인 (외부 함수에서 이미 한 번 대기했지만,
	// leader change 시점에는 follower들이 leader 역할로 전환되어야 하므로 한 번 더 대기)
	for i := range c.procs {
		if c.procs[i] != nil {
			iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 15*time.Second)
		}
	}

	// 버킷 생성 후 현재 리더에 lifecycle 설정 PUT
	const bucket = "lc-leader-change"
	c.GrantAdminOnBuckets(bucket)
	createBucketWithClient(t, c.S3Client(currentLeaderIdx), bucket)

	status := ls.signedLifecyclePut(t, c.httpURLs[currentLeaderIdx], bucket)
	require.Equal(t, http.StatusOK, status,
		"leader PUT /%s?lifecycle must return 200", bucket)

	// 생존 노드 인덱스 목록 (리더 제외)
	var survivorIdxs []int
	for i := range c.procs {
		if i != currentLeaderIdx {
			survivorIdxs = append(survivorIdxs, i)
		}
	}
	require.NotEmpty(t, survivorIdxs, "must have at least 2 surviving nodes after leader kill")

	// 리더 프로세스를 SIGKILL로 강제 종료하여 즉각 재선거 유도.
	// SIGKILL은 transfer-leader보다 더 극단적인 시나리오(즉각 crash)를 검증한다.
	t.Logf("killing leader node %d (%s) to trigger re-election", currentLeaderIdx, initialLeader)
	require.NoError(t, c.procs[currentLeaderIdx].Process.Signal(syscall.SIGKILL))
	_ = c.procs[currentLeaderIdx].Wait()
	c.procs[currentLeaderIdx] = nil // Stop() 중복 처리 방지

	// 생존 노드 양쪽에서 새 리더가 선출될 때까지 폴링.
	// getStatusJSON은 내부에서 require.NoError를 호출하므로 require.Eventually에서
	// HTTP 에러 발생 시 goroutine이 조용히 종료된다. 대신 직접 polling loop를 사용해
	// HTTP 에러를 무시하고 안전하게 재시도한다.
	tryStatusLeaderID := func(url string) string {
		resp, err := http.Get(url + "/api/cluster/status") //nolint:noctx
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		var s map[string]any
		if decErr := json.NewDecoder(resp.Body).Decode(&s); decErr != nil {
			return ""
		}
		id, _ := s["leader_id"].(string)
		state, _ := s["state"].(string)
		term, _ := s["term"].(float64)
		t.Logf("status check: leader_id=%q state=%s term=%.0f", id, state, term)
		return id
	}

	var newLeaderID string
	var newLeaderIdx int
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, idx := range survivorIdxs {
			id := tryStatusLeaderID(c.httpURLs[idx])
			if id != "" && id != initialLeader {
				newLeaderID = id
				goto foundNewLeader
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Failf(t, "no new leader elected",
		"a new leader must be elected within 30s after killing the old leader (initial=%s)", initialLeader)
foundNewLeader:

	// 새 리더 인덱스 탐색
	newLeaderIdx = -1
	for _, i := range survivorIdxs {
		if c.nodeID(i) == newLeaderID {
			newLeaderIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, newLeaderIdx, 0,
		"must locate new leader node by id; got %q", newLeaderID)
	t.Logf("new leader: node %d (%s)", newLeaderIdx, newLeaderID)

	// 새 리더에서 IAM 키 준비 대기
	iamWaitKeyReady(t, c.httpURLs[newLeaderIdx], c.accessKey, c.secretKey, 15*time.Second)

	// 새 리더에서 lifecycle 설정이 복제됐는지 확인 (ADR 0011 철칙)
	require.Eventually(t, func() bool {
		return ls.signedLifecycleGet(t, c.httpURLs[newLeaderIdx], bucket) == http.StatusOK
	}, 30*time.Second, 200*time.Millisecond,
		"new leader must see replicated lifecycle config after re-election (ADR 0011)")
}
