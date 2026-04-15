package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePolicy_Valid(t *testing.T) {
	raw := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS": ["user1"]},
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": ["arn:aws:s3:::mybucket/*"]
		}]
	}`
	p, err := ParsePolicy([]byte(raw))
	require.NoError(t, err)
	require.Len(t, p.Statement, 1)
	assert.Equal(t, "Allow", p.Statement[0].Effect)
	assert.Equal(t, []string{"user1"}, p.Statement[0].Principal.AWS)
	assert.Equal(t, []string{"s3:GetObject", "s3:PutObject"}, p.Statement[0].Action)
	assert.Equal(t, []string{"arn:aws:s3:::mybucket/*"}, p.Statement[0].Resource)
}

func TestParsePolicy_InvalidJSON(t *testing.T) {
	_, err := ParsePolicy([]byte(`{invalid`))
	assert.Error(t, err)
}

func TestParsePolicy_EmptyStatements(t *testing.T) {
	raw := `{"Version": "2012-10-17", "Statement": []}`
	p, err := ParsePolicy([]byte(raw))
	require.NoError(t, err)
	assert.Empty(t, p.Statement)
}

func TestPrincipalUnmarshal_StringForm(t *testing.T) {
	// Principal can be "*" (string) for public access
	raw := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::public/*"]
		}]
	}`
	p, err := ParsePolicy([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, []string{"*"}, p.Statement[0].Principal.AWS)
}

func TestEvaluatePolicy_AllowMatch(t *testing.T) {
	p := &BucketPolicy{
		Statement: []PolicyStatement{{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"user1"}},
			Action:    []string{"s3:GetObject"},
			Resource:  []string{"arn:aws:s3:::mybucket/*"},
		}},
	}
	assert.True(t, p.IsAllowed("user1", "s3:GetObject", "mybucket", "file.txt"))
}

func TestEvaluatePolicy_DenyOverridesAllow(t *testing.T) {
	p := &BucketPolicy{
		Statement: []PolicyStatement{
			{
				Effect:    "Allow",
				Principal: PolicyPrincipal{AWS: []string{"user1"}},
				Action:    []string{"s3:*"},
				Resource:  []string{"arn:aws:s3:::mybucket/*"},
			},
			{
				Effect:    "Deny",
				Principal: PolicyPrincipal{AWS: []string{"user1"}},
				Action:    []string{"s3:DeleteObject"},
				Resource:  []string{"arn:aws:s3:::mybucket/*"},
			},
		},
	}
	assert.True(t, p.IsAllowed("user1", "s3:GetObject", "mybucket", "file.txt"))
	assert.False(t, p.IsAllowed("user1", "s3:DeleteObject", "mybucket", "file.txt"))
}

func TestEvaluatePolicy_DefaultDeny(t *testing.T) {
	p := &BucketPolicy{
		Statement: []PolicyStatement{{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"user1"}},
			Action:    []string{"s3:GetObject"},
			Resource:  []string{"arn:aws:s3:::mybucket/*"},
		}},
	}
	// Different user → default deny
	assert.False(t, p.IsAllowed("user2", "s3:GetObject", "mybucket", "file.txt"))
	// Different action → default deny
	assert.False(t, p.IsAllowed("user1", "s3:PutObject", "mybucket", "file.txt"))
}

func TestEvaluatePolicy_WildcardPrincipal(t *testing.T) {
	p := &BucketPolicy{
		Statement: []PolicyStatement{{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"*"}},
			Action:    []string{"s3:GetObject"},
			Resource:  []string{"arn:aws:s3:::public/*"},
		}},
	}
	assert.True(t, p.IsAllowed("anyone", "s3:GetObject", "public", "file.txt"))
}

func TestEvaluatePolicy_WildcardAction(t *testing.T) {
	p := &BucketPolicy{
		Statement: []PolicyStatement{{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"admin"}},
			Action:    []string{"s3:*"},
			Resource:  []string{"arn:aws:s3:::mybucket/*"},
		}},
	}
	assert.True(t, p.IsAllowed("admin", "s3:GetObject", "mybucket", "file.txt"))
	assert.True(t, p.IsAllowed("admin", "s3:PutObject", "mybucket", "file.txt"))
	assert.True(t, p.IsAllowed("admin", "s3:DeleteObject", "mybucket", "file.txt"))
}

func TestEvaluatePolicy_BucketLevelResource(t *testing.T) {
	p := &BucketPolicy{
		Statement: []PolicyStatement{{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"user1"}},
			Action:    []string{"s3:ListBucket"},
			Resource:  []string{"arn:aws:s3:::mybucket"},
		}},
	}
	// Bucket-level action (no key)
	assert.True(t, p.IsAllowed("user1", "s3:ListBucket", "mybucket", ""))
	// Object-level resource should not match bucket-level resource pattern
	assert.False(t, p.IsAllowed("user1", "s3:ListBucket", "otherbucket", ""))
}

func TestEvaluatePolicy_EmptyPolicy(t *testing.T) {
	p := &BucketPolicy{}
	// Empty policy → default deny
	assert.False(t, p.IsAllowed("user1", "s3:GetObject", "mybucket", "file.txt"))
}
