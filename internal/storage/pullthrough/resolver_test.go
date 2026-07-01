package pullthrough

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam"
)

func TestIAMResolver_MissReturnsFalse(t *testing.T) {
	store := iam.NewStore()
	r := NewIAMResolver(store)

	if up, ok := r.Resolve("nope"); ok || up != nil {
		require.Failf(t, "Resolve(nope)", "got (%v, %v), want (nil, false)", up, ok)
	}
}

func TestIAMResolver_HitBuildsAndCachesS3Upstream(t *testing.T) {
	store := iam.NewStore()
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket:    "shared",
		Endpoint:  "http://up.example:9000",
		AccessKey: "AKUP",
		SecretKey: "sk-plain",
		CreatedAt: time.Now().UTC(),
	})
	r := NewIAMResolver(store)

	first, ok := r.Resolve("shared")
	require.True(t, ok, "first Resolve")
	require.NotNil(t, first, "first Resolve")
	second, _ := r.Resolve("shared")
	assert.Same(t, first, second, "cache miss: Resolve returned different *S3Upstream instances on repeat call")
}

func TestIAMResolver_InvalidatesOnAccessKeyRotation(t *testing.T) {
	store := iam.NewStore()
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket:    "shared",
		Endpoint:  "http://up.example:9000",
		AccessKey: "AKOLD",
		SecretKey: "old",
		CreatedAt: time.Now().UTC(),
	})
	r := NewIAMResolver(store)

	first, _ := r.Resolve("shared")
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket:    "shared",
		Endpoint:  "http://up.example:9000",
		AccessKey: "AKNEW",
		SecretKey: "new",
		CreatedAt: time.Now().UTC(),
	})
	second, ok := r.Resolve("shared")
	require.True(t, ok, "post-rotation Resolve")
	require.NotNil(t, second, "post-rotation Resolve")
	assert.NotSame(t, first, second, "cache did not invalidate after access key rotation")
}

func TestIAMResolver_InvalidatesOnSecretOnlyRotation(t *testing.T) {
	store := iam.NewStore()
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket:       "rot",
		Endpoint:     "http://up",
		AccessKey:    "AK",
		SecretKey:    "old",
		SecretKeyEnc: []byte{1, 2, 3},
		CreatedAt:    time.Now().UTC(),
	})
	r := NewIAMResolver(store)

	first, ok := r.Resolve("rot")
	require.True(t, ok, "first Resolve")
	require.NotNil(t, first, "first Resolve")

	// Same endpoint + same AK; only SecretKey + SecretKeyEnc changed
	// (typical AWS IAM secret-only rotation).
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket:       "rot",
		Endpoint:     "http://up",
		AccessKey:    "AK",
		SecretKey:    "new",
		SecretKeyEnc: []byte{4, 5, 6},
		CreatedAt:    time.Now().UTC(),
	})
	second, ok := r.Resolve("rot")
	require.True(t, ok, "post-rotation Resolve")
	require.NotNil(t, second, "post-rotation Resolve")
	assert.NotSame(t, first, second, "cache did not invalidate after secret-only rotation")
}

func TestIAMResolver_InvalidatesOnDelete(t *testing.T) {
	store := iam.NewStore()
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket: "shared", Endpoint: "http://up", AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now().UTC(),
	})
	r := NewIAMResolver(store)
	if _, ok := r.Resolve("shared"); !ok {
		require.Fail(t, "seed Resolve")
	}
	store.ApplyBucketUpstreamDeleteForTest("shared")
	if up, ok := r.Resolve("shared"); ok || up != nil {
		assert.Failf(t, "Resolve after delete", "got (%v, %v), want (nil, false)", up, ok)
	}
}

// A4 verification: the warn log path is gated by the build-error branch and
// must not fire on the success path nor on the silent-miss (deleted) path.
func TestIAMResolver_BuildFailureLogsWarning(t *testing.T) {
	var buf bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&buf).Level(zerolog.WarnLevel)
	defer func() { log.Logger = prevLogger }()

	store := iam.NewStore()
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket: "shared", Endpoint: "http://up", AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now().UTC(),
	})
	r := NewIAMResolver(store)
	if _, ok := r.Resolve("shared"); !ok {
		require.Fail(t, "Resolve should succeed on valid record")
	}
	if buf.Len() > 0 && strings.Contains(buf.String(), "build upstream failed") {
		assert.Failf(t, "warning logged on success path", "%s", buf.String())
	}
	// Negative path: deleted record → silent miss, no warn.
	store.ApplyBucketUpstreamDeleteForTest("shared")
	buf.Reset()
	if _, ok := r.Resolve("shared"); ok {
		assert.Fail(t, "Resolve(deleted) should be miss")
	}
	if strings.Contains(buf.String(), "build upstream failed") {
		assert.Failf(t, "warning logged on deleted-record miss path", "%s", buf.String())
	}
}
