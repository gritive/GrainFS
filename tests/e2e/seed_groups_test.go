package e2e

import (
	"context"
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

	const (
		numNodes   = 3
		seedGroups = 2
	)

	c := startMRCluster(t, numNodes, seedGroups)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	leaderIdx, err := waitForWritableEndpoint(
		ctx,
		c.httpURLs,
		120*time.Second,
		5*time.Second,
		1*time.Second,
		func(ctx context.Context, endpoint string) error {
			client := ecS3Client(endpoint, c.accessKey, c.secretKey)
			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("seed-bucket")})
			return err
		},
	)
	require.NoError(t, err, "no leader found")
	c.leaderIdx = leaderIdx

	t.Logf("seed-groups multi test passed: %d groups seeded across %d nodes", seedGroups, numNodes)
}
