package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
func (ls lifecycleSigner) signedLifecyclePut(t testing.TB, baseURL, bucket string) int {
	t.Helper()
	ctx := context.Background()
	body := []byte(lifecycleXML)
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])

	url := baseURL + "/" + bucket + "?lifecycle"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = ls.signer.SignHTTP(ctx, ls.creds, req, payloadHash, "s3", "us-east-1", time.Now())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Logf("PUT %s?lifecycle -> %d: %s", bucket, resp.StatusCode, string(respBody))
	}
	return resp.StatusCode
}

// signedLifecycleGet는 SigV4로 서명된 GET /<bucket>?lifecycle 요청을 발송하고 상태 코드를 반환한다.
func (ls lifecycleSigner) signedLifecycleGet(t testing.TB, baseURL, bucket string) int {
	t.Helper()
	ctx := context.Background()
	sum := sha256.Sum256(nil)
	payloadHash := hex.EncodeToString(sum[:])

	url := baseURL + "/" + bucket + "?lifecycle"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = ls.signer.SignHTTP(ctx, ls.creds, req, payloadHash, "s3", "us-east-1", time.Now())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

// waitClusterSettled는 3-voter 클러스터가 안정될 때까지 기다린다.
// getStatusJSON이 "peers" 2개를 보고할 때 완전히 합류한 것으로 간주.
func waitClusterSettled(t testing.TB, leaderURL string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		s := getStatusJSON(t, leaderURL)
		return len(stringList(s["peers"])) == 2
	}).WithTimeout(90*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(),
		"3-node cluster must settle (peers==2)")
}

// TestLifecycleE2E collapses lifecycle replication checks (follower→leader
// PUT forward, leader-change preserves replicated config) into one entry.
// Both sub-specs need a 3-voter DynamicJoin cluster — the second one kills
// the leader, so it runs last in Ordered Context order; the first sub-spec
// runs against the pristine cluster.
func TestLifecycleE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Lifecycle replication e2e")
}

var _ = ginkgo.Describe("Lifecycle replication", func() {
	ginkgo.Context("Cluster3Node", ginkgo.Ordered, func() {
		var c *e2eCluster
		var ls lifecycleSigner

		ginkgo.BeforeAll(func() {
			tb := ginkgo.GinkgoTB()
			c = startE2ECluster(tb, e2eClusterOptions{
				Nodes:      3,
				Mode:       ClusterModeDynamicJoin,
				ClusterKey: "E2E-LIFECYCLE-REPL-KEY",
				LogPrefix:  "grainfs-lifecycle",
				ExtraArgs:  []string{"--lifecycle-interval=24h"},
			})
			gomega.Expect(c.leaderIdx).To(gomega.BeNumerically(">=", 0),
				"harness must have identified leader")
			waitClusterSettled(tb, c.httpURLs[c.leaderIdx])

			for i := range c.procs {
				iamWaitKeyReady(tb, c.httpURLs[i], c.accessKey, c.secretKey, 15*time.Second)
			}
			ls = newLifecycleSigner(c.accessKey, c.secretKey)
		})

		ginkgo.It("follower PUT forwards to leader (FollowerPutLeaderGet)", func(ctx context.Context) {
			tb := ginkgo.GinkgoTB()
			leaderIdx := c.leaderIdx
			followerIdx := -1
			for i := range c.procs {
				if i != leaderIdx {
					followerIdx = i
					break
				}
			}
			gomega.Expect(followerIdx).To(gomega.BeNumerically(">=", 0), "must find a follower node")

			const bucket = "lc-follower-put"
			createBucketWithAdminPolicyAttachViaUDSAny(tb, c.dataDirs, c.saID, bucket, c.S3Client(leaderIdx))

			status := ls.signedLifecyclePut(tb, c.httpURLs[followerIdx], bucket)
			gomega.Expect(status).To(gomega.Equal(http.StatusOK),
				"follower PUT /%s?lifecycle must return 200 (ADR 0011: follower must forward to leader)", bucket)

			gomega.Eventually(func() bool {
				return ls.signedLifecycleGet(tb, c.httpURLs[leaderIdx], bucket) == http.StatusOK
			}).WithTimeout(5*time.Second).WithPolling(100*time.Millisecond).Should(gomega.BeTrue(),
				"leader must observe replicated lifecycle config within 5s")
		}, ginkgo.NodeTimeout(180*time.Second))

		// destructive: kills the current leader to force re-election. Must run
		// last in Ordered Context order — subsequent It blocks would see a
		// post-leader-kill cluster.
		ginkgo.It("preserves config across leader change (LeaderChangePreservesConfig)", func(ctx context.Context) {
			tb := ginkgo.GinkgoTB()
			leaderIdx := c.leaderIdx
			s := getStatusJSON(tb, c.httpURLs[leaderIdx])
			initialLeader, _ := s["leader_id"].(string)
			gomega.Expect(initialLeader).NotTo(gomega.BeEmpty(),
				"must resolve current leader id from status")

			// leader_id → 노드 인덱스 매핑
			currentLeaderIdx := -1
			for i := range c.procs {
				if c.nodeID(i) == initialLeader {
					currentLeaderIdx = i
					break
				}
			}
			gomega.Expect(currentLeaderIdx).To(gomega.BeNumerically(">=", 0),
				"must locate current leader by node id")

			// 새 리더 후보가 IAM 키를 이미 받았는지 재확인 (BeforeAll에서 이미 한 번 대기했지만,
			// leader change 시점에는 follower들이 leader 역할로 전환되어야 하므로 한 번 더 대기)
			for i := range c.procs {
				if c.procs[i] != nil {
					iamWaitKeyReady(tb, c.httpURLs[i], c.accessKey, c.secretKey, 15*time.Second)
				}
			}

			// 버킷 생성 후 현재 리더에 lifecycle 설정 PUT
			const bucket = "lc-leader-change"
			createBucketWithAdminPolicyAttachViaUDSAny(tb, c.dataDirs, c.saID, bucket, c.S3Client(currentLeaderIdx))

			status := ls.signedLifecyclePut(tb, c.httpURLs[currentLeaderIdx], bucket)
			gomega.Expect(status).To(gomega.Equal(http.StatusOK),
				"leader PUT /%s?lifecycle must return 200", bucket)

			// 생존 노드 인덱스 목록 (리더 제외)
			var survivorIdxs []int
			for i := range c.procs {
				if i != currentLeaderIdx {
					survivorIdxs = append(survivorIdxs, i)
				}
			}
			gomega.Expect(survivorIdxs).NotTo(gomega.BeEmpty(),
				"must have at least 2 surviving nodes after leader kill")

			// 리더 프로세스를 SIGKILL로 강제 종료하여 즉각 재선거 유도.
			// SIGKILL은 transfer-leader보다 더 극단적인 시나리오(즉각 crash)를 검증한다.
			tb.Logf("killing leader node %d (%s) to trigger re-election", currentLeaderIdx, initialLeader)
			gomega.Expect(c.procs[currentLeaderIdx].Process.Signal(syscall.SIGKILL)).NotTo(gomega.HaveOccurred())
			_ = c.procs[currentLeaderIdx].Wait()
			// 죽은 leader proc 슬롯의 nil 마킹은 spec 종료 후로 미룬다 —
			// 본문은 currentLeaderIdx 슬롯을 더 이상 참조하지 않으니 안전하고,
			// BeforeAll의 cluster cleanup(t.Cleanup)이 호출될 때 helper의 Stop()이
			// 이미 죽은 proc을 두 번 정리하지 않도록 ginkgo.DeferCleanup으로 명시.
			killedIdx := currentLeaderIdx
			ginkgo.DeferCleanup(func() {
				c.procs[killedIdx] = nil
			})

			// 생존 노드 양쪽에서 새 리더가 선출될 때까지 폴링.
			tryStatusLeaderID := func(url string) string {
				resp, err := http.Get(url + "/api/cluster/status") //nolint:noctx
				if err != nil {
					return ""
				}
				defer resp.Body.Close()
				var ss map[string]any
				if decErr := json.NewDecoder(resp.Body).Decode(&ss); decErr != nil {
					return ""
				}
				id, _ := ss["leader_id"].(string)
				state, _ := ss["state"].(string)
				term, _ := ss["term"].(float64)
				tb.Logf("status check: leader_id=%q state=%s term=%.0f", id, state, term)
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
			ginkgo.Fail(fmt.Sprintf("a new leader must be elected within 30s after killing the old leader (initial=%s)", initialLeader))
		foundNewLeader:

			// 새 리더 인덱스 탐색
			newLeaderIdx = -1
			for _, i := range survivorIdxs {
				if c.nodeID(i) == newLeaderID {
					newLeaderIdx = i
					break
				}
			}
			gomega.Expect(newLeaderIdx).To(gomega.BeNumerically(">=", 0),
				"must locate new leader node by id; got %q", newLeaderID)
			tb.Logf("new leader: node %d (%s)", newLeaderIdx, newLeaderID)

			// 새 리더에서 IAM 키 준비 대기
			iamWaitKeyReady(tb, c.httpURLs[newLeaderIdx], c.accessKey, c.secretKey, 15*time.Second)

			// 새 리더에서 lifecycle 설정이 복제됐는지 확인 (ADR 0011 철칙)
			gomega.Eventually(func() bool {
				return ls.signedLifecycleGet(tb, c.httpURLs[newLeaderIdx], bucket) == http.StatusOK
			}).WithTimeout(30*time.Second).WithPolling(200*time.Millisecond).Should(gomega.BeTrue(),
				"new leader must see replicated lifecycle config after re-election (ADR 0011)")
		}, ginkgo.NodeTimeout(240*time.Second))
	})
})
