package server

import (
	"context"
	"encoding/json"
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

func TestCompiledPolicyStore_ImplementsAuthorizer(t *testing.T) {
	var _ s3auth.Authorizer = NewCompiledPolicyStore()
}
