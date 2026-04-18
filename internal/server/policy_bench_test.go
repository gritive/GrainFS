package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

var benchActions = []string{
	"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:CreateBucket",
}

// makeMixedPolicy creates a policy with n statements across 5 different actions.
// The matching statement (benchuser + GetObject) is at position n-1.
func makeMixedPolicy(t testing.TB, n int) []byte {
	t.Helper()
	stmts := make([]PolicyStatement, n)
	for i := range stmts {
		action := benchActions[i%len(benchActions)]
		stmts[i] = PolicyStatement{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"user" + string(rune('0'+i%10))}},
			Action:    []string{action},
			Resource:  []string{"arn:aws:s3:::bucket/*"},
		}
	}
	stmts[n-1] = allowStmt("benchuser", "s3:GetObject", "arn:aws:s3:::bench/*")

	data, err := json.Marshal(BucketPolicy{Version: "2012-10-17", Statement: stmts})
	require.NoError(t, err)
	return data
}

// makeLargePolicy creates a policy where ALL statements use the same action (worst case for compiled).
func makeLargePolicy(t testing.TB, n int) []byte {
	t.Helper()
	stmts := make([]PolicyStatement, n)
	for i := range stmts {
		stmts[i] = PolicyStatement{
			Effect:    "Allow",
			Principal: PolicyPrincipal{AWS: []string{"user" + string(rune('0'+i%10))}},
			Action:    []string{"s3:GetObject"},
			Resource:  []string{"arn:aws:s3:::bucket/*"},
		}
	}
	stmts[n-1] = allowStmt("benchuser", "s3:GetObject", "arn:aws:s3:::bench/*")

	data, err := json.Marshal(BucketPolicy{Version: "2012-10-17", Statement: stmts})
	require.NoError(t, err)
	return data
}

func benchmarkOld(b *testing.B, n int, policy func(testing.TB, int) []byte) {
	b.Helper()
	ps := NewPolicyStore()
	require.NoError(b, ps.Set("bench", policy(b, n)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ps.IsAllowed("benchuser", "s3:GetObject", "bench", "key")
	}
}

func benchmarkNew(b *testing.B, n int, policy func(testing.TB, int) []byte) {
	b.Helper()
	cs := NewCompiledPolicyStore()
	require.NoError(b, cs.Set("bench", policy(b, n)))
	ctx := context.Background()
	in := s3auth.PermCheckInput{
		Principal: s3auth.Principal{AccessKey: "benchuser"},
		Resource:  s3auth.ResourceRef{Bucket: "bench", Key: "key"},
		Action:    s3auth.GetObject,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cs.Allow(ctx, in)
	}
}

// Same-action: worst case for CompiledPolicyStore (all stmts in same action bucket).
func BenchmarkOld_SameAction_10(b *testing.B)  { benchmarkOld(b, 10, makeLargePolicy) }
func BenchmarkNew_SameAction_10(b *testing.B)  { benchmarkNew(b, 10, makeLargePolicy) }
func BenchmarkOld_SameAction_50(b *testing.B)  { benchmarkOld(b, 50, makeLargePolicy) }
func BenchmarkNew_SameAction_50(b *testing.B)  { benchmarkNew(b, 50, makeLargePolicy) }
func BenchmarkOld_SameAction_100(b *testing.B) { benchmarkOld(b, 100, makeLargePolicy) }
func BenchmarkNew_SameAction_100(b *testing.B) { benchmarkNew(b, 100, makeLargePolicy) }

// Mixed-action: typical real-world case.
func BenchmarkOld_Mixed_10(b *testing.B)  { benchmarkOld(b, 10, makeMixedPolicy) }
func BenchmarkNew_Mixed_10(b *testing.B)  { benchmarkNew(b, 10, makeMixedPolicy) }
func BenchmarkOld_Mixed_50(b *testing.B)  { benchmarkOld(b, 50, makeMixedPolicy) }
func BenchmarkNew_Mixed_50(b *testing.B)  { benchmarkNew(b, 50, makeMixedPolicy) }
func BenchmarkOld_Mixed_100(b *testing.B) { benchmarkOld(b, 100, makeMixedPolicy) }
func BenchmarkNew_Mixed_100(b *testing.B) { benchmarkNew(b, 100, makeMixedPolicy) }
