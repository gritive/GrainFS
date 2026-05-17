package policy

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
func BenchmarkNew_SameAction_10(b *testing.B)  { benchmarkNew(b, 10, makeLargePolicy) }
func BenchmarkNew_SameAction_50(b *testing.B)  { benchmarkNew(b, 50, makeLargePolicy) }
func BenchmarkNew_SameAction_100(b *testing.B) { benchmarkNew(b, 100, makeLargePolicy) }

// Mixed-action: typical real-world case.
func BenchmarkNew_Mixed_10(b *testing.B)  { benchmarkNew(b, 10, makeMixedPolicy) }
func BenchmarkNew_Mixed_50(b *testing.B)  { benchmarkNew(b, 50, makeMixedPolicy) }
func BenchmarkNew_Mixed_100(b *testing.B) { benchmarkNew(b, 100, makeMixedPolicy) }

// BenchmarkParallelAllow measures Allow under read-only parallel load with
// N buckets pre-populated. This is the workload where RWMutex.RLock acquire/
// release overhead vs. lock-free atomic.Load shows up. The audit doc names
// the trigger: "request evaluation uses short read locks" — measure them.
func BenchmarkParallelAllow(b *testing.B) {
	cs := NewCompiledPolicyStore()
	const buckets = 100
	policy := makeLargePolicy(b, 5)
	for i := 0; i < buckets; i++ {
		require.NoError(b, cs.Set(bucketName(i), policy))
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cs.Allow(ctx, s3auth.PermCheckInput{
				Principal: s3auth.Principal{AccessKey: "benchuser"},
				Resource:  s3auth.ResourceRef{Bucket: bucketName(i % buckets), Key: "key"},
				Action:    s3auth.GetObject,
			})
			i++
		}
	})
}

// BenchmarkParallelAllowWithWriter simulates a low-rate writer (Set every
// ~100ms) while many readers run. RWMutex writer-priority can starve readers
// when policy mutations happen mid-traffic.
func BenchmarkParallelAllowWithWriter(b *testing.B) {
	cs := NewCompiledPolicyStore()
	const buckets = 100
	policy := makeLargePolicy(b, 5)
	for i := 0; i < buckets; i++ {
		require.NoError(b, cs.Set(bucketName(i), policy))
	}
	ctx := context.Background()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		i := buckets
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				_ = cs.Set(bucketName(i), policy)
				i++
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cs.Allow(ctx, s3auth.PermCheckInput{
				Principal: s3auth.Principal{AccessKey: "benchuser"},
				Resource:  s3auth.ResourceRef{Bucket: bucketName(i % buckets), Key: "key"},
				Action:    s3auth.GetObject,
			})
			i++
		}
	})
}

func bucketName(i int) string {
	return "bench-" + string(rune('a'+i%26)) + "-" + string(rune('0'+(i/26)%10))
}
