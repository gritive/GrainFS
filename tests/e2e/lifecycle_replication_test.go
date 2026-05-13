package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
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

// TestLifecycle_FollowerPutLeaderGet: 팔로워에 PUT한 lifecycle 설정이
// 리더에서 GET으로 조회 가능해야 한다 (ADR 0011 복제 요건).
func TestLifecycle_FollowerPutLeaderGet(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	if _, err := os.Stat(getBinary()); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", getBinary())
	}

	// lifecycle-interval을 24h로 설정: 서비스와 메타데이터 복제는 활성화하되
	// 주기적 executor는 거의 실행되지 않도록 함 (0=disable이므로 0이 아닌 값 필요)
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeDynamicJoin,
		ClusterKey: "E2E-LIFECYCLE-REPL-KEY",
		LogPrefix:  "grainfs-lifecycle-follower-put",
		ExtraArgs:  []string{"--lifecycle-interval=24h"},
	})

	leaderIdx := c.leaderIdx
	require.GreaterOrEqual(t, leaderIdx, 0, "harness must have identified leader")

	waitClusterSettled(t, c.httpURLs[leaderIdx])

	// 팔로워 인덱스 선택 (리더가 아닌 첫 번째 노드)
	followerIdx := -1
	for i := range c.procs {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, followerIdx, 0, "must find a follower node")

	// IAM 키가 모든 노드에 전파될 때까지 대기
	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 15*time.Second)
	}

	// 버킷 생성 (리더 노드의 S3 클라이언트 사용)
	const bucket = "lc-follower-put"
	c.GrantAdminOnBuckets(bucket)
	createBucketWithClient(t, c.S3Client(leaderIdx), bucket)

	ls := newLifecycleSigner(c.accessKey, c.secretKey)

	// 팔로워에 lifecycle 설정 PUT
	status := ls.signedLifecyclePut(t, c.httpURLs[followerIdx], bucket)
	require.Equal(t, http.StatusOK, status,
		"follower PUT /%s?lifecycle must return 200 (ADR 0011: follower must forward to leader)", bucket)

	// 리더에서 eventually-200 GET 확인 (복제 대기)
	require.Eventually(t, func() bool {
		return ls.signedLifecycleGet(t, c.httpURLs[leaderIdx], bucket) == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond,
		"leader must observe replicated lifecycle config within 5s")
}

// TestLifecycle_LeaderChangePreservesConfig: 리더 교체 이후에도 lifecycle 설정이
// 새 리더에서 조회 가능해야 한다 (ADR 0011 철칙).
//
// 주의: transfer-leader CLI는 devel 브랜치에서 "cluster adapter does not support
// transfer-leader" 오류로 실패하는 기존 버그가 있다. 이 테스트는 transfer-leader를
// 사용하지 않고, 리더 프로세스를 SIGKILL로 강제 종료한 뒤 나머지 노드들이 재선거
// (re-election)를 완료하길 기다리는 방식으로 동일 시나리오를 검증한다.
func TestLifecycle_LeaderChangePreservesConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	if _, err := os.Stat(getBinary()); err != nil {
		t.Skipf("grainfs binary not found at %s — run `make build` first", getBinary())
	}

	// lifecycle-interval을 24h로 설정: 서비스와 메타데이터 복제는 활성화하되
	// 주기적 executor는 거의 실행되지 않도록 함 (0=disable이므로 0이 아닌 값 필요)
	//
	// ClusterModeStaticPeers 사용 이유: DynamicJoin 모드에서는 joining 노드가
	// ConfChange(AddLearner+Promote) 시 자신의 raft 주소를 config.Peers에
	// 추가하여 cluster quorum이 4가 된다. 리더 사망 후 생존 노드 2개가
	// quorum(3) 미달로 재선거에 실패한다. StaticPeers는 초기 config.Peers가
	// 올바르게 설정되어 이 문제를 회피한다.
	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		Mode:       ClusterModeStaticPeers,
		ClusterKey: "E2E-LIFECYCLE-LEADER-CHANGE-KEY",
		LogPrefix:  "grainfs-lifecycle-leader-change",
		ExtraArgs:  []string{"--lifecycle-interval=24h"},
	})

	leaderIdx := c.leaderIdx
	require.GreaterOrEqual(t, leaderIdx, 0, "harness must have identified leader")

	// 클러스터 안정 대기 + 현재 리더 ID 확보
	waitClusterSettled(t, c.httpURLs[leaderIdx])

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

	// IAM 키가 모든 노드에 전파될 때까지 대기
	for i := range c.procs {
		iamWaitKeyReady(t, c.httpURLs[i], c.accessKey, c.secretKey, 15*time.Second)
	}

	// 버킷 생성 후 현재 리더에 lifecycle 설정 PUT
	const bucket = "lc-leader-change"
	c.GrantAdminOnBuckets(bucket)
	createBucketWithClient(t, c.S3Client(currentLeaderIdx), bucket)

	ls := newLifecycleSigner(c.accessKey, c.secretKey)

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
	// transfer-leader 대신 프로세스 킬을 사용하는 이유: transfer-leader CLI가
	// 현재 devel 브랜치에서 기존 버그로 실패함 (pre-existing regression).
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
