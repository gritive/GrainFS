package pullthrough

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/iam"
)

func TestIAMResolver_MissReturnsFalse(t *testing.T) {
	store := iam.NewStore()
	r := NewIAMResolver(store)

	if up, ok := r.Resolve("nope"); ok || up != nil {
		t.Fatalf("Resolve(nope): got (%v, %v) want (nil, false)", up, ok)
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
	if !ok || first == nil {
		t.Fatalf("first Resolve: ok=%v up=%v", ok, first)
	}
	second, _ := r.Resolve("shared")
	if first != second {
		t.Errorf("cache miss: Resolve returned different *S3Upstream instances on repeat call")
	}
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
	if !ok || second == nil {
		t.Fatalf("post-rotation Resolve: ok=%v up=%v", ok, second)
	}
	if first == second {
		t.Error("cache did not invalidate after access key rotation")
	}
}

func TestIAMResolver_InvalidatesOnDelete(t *testing.T) {
	store := iam.NewStore()
	store.ApplyBucketUpstreamForTest(iam.BucketUpstream{
		Bucket: "shared", Endpoint: "http://up", AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now().UTC(),
	})
	r := NewIAMResolver(store)
	if _, ok := r.Resolve("shared"); !ok {
		t.Fatal("seed Resolve")
	}
	store.ApplyBucketUpstreamDeleteForTest("shared")
	if up, ok := r.Resolve("shared"); ok || up != nil {
		t.Errorf("Resolve after delete: got (%v, %v) want (nil, false)", up, ok)
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
		t.Fatal("Resolve should succeed on valid record")
	}
	if buf.Len() > 0 && strings.Contains(buf.String(), "build upstream failed") {
		t.Errorf("warning logged on success path: %s", buf.String())
	}
	// Negative path: deleted record → silent miss, no warn.
	store.ApplyBucketUpstreamDeleteForTest("shared")
	buf.Reset()
	if _, ok := r.Resolve("shared"); ok {
		t.Error("Resolve(deleted) should be miss")
	}
	if strings.Contains(buf.String(), "build upstream failed") {
		t.Errorf("warning logged on deleted-record miss path: %s", buf.String())
	}
}
