package policy

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func makePolicy(t *testing.T, statements ...PolicyStatement) []byte {
	t.Helper()
	data, err := json.Marshal(BucketPolicy{
		Version:   "2012-10-17",
		Statement: statements,
	})
	require.NoError(t, err)
	return data
}

func allowStmt(principal, action, resource string) PolicyStatement {
	return PolicyStatement{
		Effect:    "Allow",
		Principal: PolicyPrincipal{AWS: []string{principal}},
		Action:    []string{action},
		Resource:  []string{resource},
	}
}

func denyStmt(principal, action, resource string) PolicyStatement {
	return PolicyStatement{
		Effect:    "Deny",
		Principal: PolicyPrincipal{AWS: []string{principal}},
		Action:    []string{action},
		Resource:  []string{resource},
	}
}

func inp(accessKey string, action s3auth.S3Action, bucket, key string) s3auth.PermCheckInput {
	return s3auth.PermCheckInput{
		Principal: s3auth.Principal{AccessKey: accessKey},
		Resource:  s3auth.ResourceRef{Bucket: bucket, Key: key},
		Action:    action,
	}
}

// denyPutPolicy builds a Deny-PutObject-for-* bucket policy as raw JSON, reusing
// the existing test helpers so the pull-on-miss loader can hand it back.
func denyPutPolicy(t *testing.T, bucket string) []byte {
	t.Helper()
	return makePolicy(t, denyStmt("*", "s3:PutObject", "arn:aws:s3:::"+bucket+"/*"))
}

func TestAllowPullsOnMissAndEnforcesDeny(t *testing.T) {
	cs := NewCompiledPolicyStore()
	var calls int
	cs.SetLoader(func(bucket string) ([]byte, bool, error) {
		calls++
		if bucket == "b" {
			return denyPutPolicy(t, "b"), true, nil
		}
		return nil, false, nil
	})

	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // committed Deny enforced on cold cache
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // served from cache
	require.Equal(t, 1, calls, "policy bucket resolved once then cached")
}

func TestAllowNegativeCachesNoPolicy(t *testing.T) {
	cs := NewCompiledPolicyStore()
	var calls int
	cs.SetLoader(func(string) ([]byte, bool, error) { calls++; return nil, false, nil })
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "free", "k")))
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "free", "k")))
	require.Equal(t, 1, calls, "no-policy bucket resolved once then negative-cached")
}

func TestAllowNilLoaderPreservesLegacyDefaultAllow(t *testing.T) {
	cs := NewCompiledPolicyStore() // no loader
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
}

func TestAllowOnReadFaultAllowsUncached(t *testing.T) {
	cs := NewCompiledPolicyStore()
	var calls int
	cs.SetLoader(func(string) ([]byte, bool, error) { calls++; return nil, false, errors.New("badger down") })
	// Indeterminate read → legacy allow, and NOT cached (so it retries).
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
	require.Equal(t, 2, calls, "fault is not cached; re-read each request")
}

func TestAllowFailsClosedOnMalformedPolicy(t *testing.T) {
	cs := NewCompiledPolicyStore()
	var calls int
	cs.SetLoader(func(string) ([]byte, bool, error) { calls++; return []byte("{not json"), true, nil })
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // unparseable committed policy → deny
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
	require.Equal(t, 1, calls, "malformed policy cached as deny, not re-read")
}

func TestSetClearsNegativeCache(t *testing.T) {
	cs := NewCompiledPolicyStore()
	cs.SetLoader(func(string) ([]byte, bool, error) { return nil, false, nil })
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))  // negative-cached
	require.NoError(t, cs.Set("b", denyPutPolicy(t, "b")))                                    // write-path Set on same node
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // Deny now enforced, no Invalidate needed
}

func TestInvalidateTightensCachedAllowToDeny(t *testing.T) {
	cs := NewCompiledPolicyStore()
	deny := false
	cs.SetLoader(func(string) ([]byte, bool, error) {
		if deny {
			return denyPutPolicy(t, "b"), true, nil
		}
		return nil, false, nil
	})
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // no policy → allow + negative-cache
	deny = true
	cs.Invalidate("b")                                                                        // policy tightened on another node (apply-hook)
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // re-pull → Deny enforced
}

func TestInvalidateLoosensCachedDenyToAllow(t *testing.T) {
	cs := NewCompiledPolicyStore()
	deny := true
	cs.SetLoader(func(string) ([]byte, bool, error) {
		if deny {
			return denyPutPolicy(t, "b"), true, nil
		}
		return nil, false, nil
	})
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
	deny = false
	cs.Invalidate("b") // policy deleted on another node
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
}

func TestInvalidateAllFlushesEverything(t *testing.T) {
	cs := NewCompiledPolicyStore()
	live := true
	cs.SetLoader(func(string) ([]byte, bool, error) {
		if live {
			return denyPutPolicy(t, "b"), true, nil
		}
		return nil, false, nil
	})
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
	live = false
	cs.Invalidate("") // snapshot install flush-all
	require.True(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")))
}

// TestInvalidateDuringInflightPullDropsStaleResult: an Invalidate that races an
// in-flight pull must win — the stale pulled view must not get cached.
func TestInvalidateDuringInflightPullDropsStaleResult(t *testing.T) {
	cs := NewCompiledPolicyStore()
	denyBytes := denyPutPolicy(t, "b") // precompute off the loader goroutine
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	deny := false
	cs.SetLoader(func(string) ([]byte, bool, error) {
		once.Do(func() { close(entered); <-release }) // only the first pull blocks
		if deny {
			return denyBytes, true, nil
		}
		return nil, false, nil // first pull returns the STALE (pre-tighten) view
	})
	done := make(chan bool)
	go func() { done <- cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k")) }()
	<-entered
	deny = true
	cs.Invalidate("b") // tighten while the pull holds the old view
	close(release)
	<-done                                                                                    // the in-flight pull may return allow for ITS request...
	require.False(t, cs.Allow(context.Background(), inp("AKIA", s3auth.PutObject, "b", "k"))) // ...but the stale result must not have been cached
}

func TestCompiledPolicyStore_NoPolicyAllowsAll(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	assert.True(t, cs.Allow(ctx, inp("user1", s3auth.GetObject, "mybucket", "key")))
	assert.True(t, cs.Allow(ctx, inp("", s3auth.GetObject, "mybucket", "key")))
	assert.True(t, cs.Allow(ctx, inp("user1", s3auth.DeleteObject, "mybucket", "key")))
}

func TestCompiledPolicyStore_ExplicitAllowGrantsAccess(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	err := cs.Set("mybucket", makePolicy(t,
		allowStmt("*", "s3:GetObject", "arn:aws:s3:::mybucket/*"),
	))
	require.NoError(t, err)

	assert.True(t, cs.Allow(ctx, inp("anyone", s3auth.GetObject, "mybucket", "file.txt")))
	assert.True(t, cs.Allow(ctx, inp("", s3auth.GetObject, "mybucket", "file.txt")))
}

func TestCompiledPolicyStore_ExplicitDenyBlocks(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	err := cs.Set("mybucket", makePolicy(t,
		denyStmt("*", "s3:DeleteObject", "arn:aws:s3:::mybucket/*"),
	))
	require.NoError(t, err)

	// delete is denied
	assert.False(t, cs.Allow(ctx, inp("admin", s3auth.DeleteObject, "mybucket", "key")))
	// but get is allowed (no matching statement when policy exists → default deny)
	// Actually: policy exists but GetObject has no statement → false (default deny)
	assert.False(t, cs.Allow(ctx, inp("admin", s3auth.GetObject, "mybucket", "key")))
}

func TestCompiledPolicyStore_DenyOverridesAllow(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	// Policy allows GetObject for everyone but denies for "baduser"
	err := cs.Set("mybucket", makePolicy(t,
		allowStmt("*", "s3:GetObject", "arn:aws:s3:::mybucket/*"),
		denyStmt("baduser", "s3:GetObject", "arn:aws:s3:::mybucket/*"),
	))
	require.NoError(t, err)

	assert.True(t, cs.Allow(ctx, inp("gooduser", s3auth.GetObject, "mybucket", "file.txt")))
	assert.False(t, cs.Allow(ctx, inp("baduser", s3auth.GetObject, "mybucket", "file.txt")))
}

func TestCompiledPolicyStore_NoMatchingStatementDenies(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	// Policy only allows GetObject; PutObject has no statement
	err := cs.Set("mybucket", makePolicy(t,
		allowStmt("user1", "s3:GetObject", "arn:aws:s3:::mybucket/*"),
	))
	require.NoError(t, err)

	assert.True(t, cs.Allow(ctx, inp("user1", s3auth.GetObject, "mybucket", "k")))
	assert.False(t, cs.Allow(ctx, inp("user1", s3auth.PutObject, "mybucket", "k")))
}

func TestCompiledPolicyStore_DeleteRestoresNoPolicy(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	err := cs.Set("mybucket", makePolicy(t,
		denyStmt("*", "s3:GetObject", "arn:aws:s3:::mybucket/*"),
	))
	require.NoError(t, err)
	assert.False(t, cs.Allow(ctx, inp("user1", s3auth.GetObject, "mybucket", "k")))

	cs.Delete("mybucket")
	// After delete: no policy → allow all
	assert.True(t, cs.Allow(ctx, inp("user1", s3auth.GetObject, "mybucket", "k")))
}

func TestCompiledPolicyStore_SetCopiesRawPolicy(t *testing.T) {
	cs := NewCompiledPolicyStore()
	raw := makePolicy(t, allowStmt("*", "s3:GetObject", "arn:aws:s3:::mybucket/*"))
	want := append([]byte(nil), raw...)

	require.NoError(t, cs.Set("mybucket", raw))
	for i := range raw {
		raw[i] = 0
	}

	require.Equal(t, want, cs.GetRaw("mybucket"))
}

func TestCompiledPolicyStore_GetRawReturnsCopy(t *testing.T) {
	cs := NewCompiledPolicyStore()
	raw := makePolicy(t, allowStmt("*", "s3:GetObject", "arn:aws:s3:::mybucket/*"))
	require.NoError(t, cs.Set("mybucket", raw))

	got := cs.GetRaw("mybucket")
	require.NotEmpty(t, got)
	got[0] = 0

	require.Equal(t, raw, cs.GetRaw("mybucket"))
}

func TestCompiledPolicyStore_WildcardAction(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	err := cs.Set("mybucket", makePolicy(t,
		allowStmt("admin", "s3:*", "arn:aws:s3:::mybucket/*"),
	))
	require.NoError(t, err)

	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.GetObject, "mybucket", "k")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.DeleteObject, "mybucket", "k")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.PutObject, "mybucket", "k")))
}

func TestCompiledPolicyStore_AllowsVersioningAndRetentionActions(t *testing.T) {
	cs := NewCompiledPolicyStore()
	ctx := context.Background()

	doc := makePolicy(t, PolicyStatement{
		Effect:    "Allow",
		Principal: PolicyPrincipal{AWS: []string{"admin"}},
		Action: []string{
			"s3:GetBucketVersioning",
			"s3:PutBucketVersioning",
			"s3:ListBucketVersions",
			"s3:GetObjectRetention",
			"s3:PutObjectRetention",
			"s3:GetBucketObjectLockConfiguration",
		},
		Resource: []string{"arn:aws:s3:::mybucket/*"},
	})
	require.NoError(t, cs.Set("mybucket", doc))

	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.GetBucketVersioning, "mybucket", "")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.PutBucketVersioning, "mybucket", "")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.ListBucketVersions, "mybucket", "")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.GetObjectRetention, "mybucket", "k")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.PutObjectRetention, "mybucket", "k")))
	assert.True(t, cs.Allow(ctx, inp("admin", s3auth.GetBucketObjectLockConfiguration, "mybucket", "")))
}

func TestCompiledPolicyStore_ImplementsPolicyChecker(t *testing.T) {
	var _ s3auth.PolicyChecker = NewCompiledPolicyStore()
}
