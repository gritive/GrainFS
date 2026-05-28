package s3auth

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkVerify_NoDecryptPerRequest locks the post-R2 invariant that the
// sigv4 verify hot path consults the IAM cache directly — it does NOT call
// UnwrapSecret per request. The R2 migration moved IAM credentials to the
// gen-aware DEK seam, but the in-memory plaintext cache was preserved so
// sigv4 verify stays decrypt-free. A regression that adds a per-request
// Open() to the verify chain will balloon allocations beyond the empirical
// ceiling enforced by TestVerify_AllocCeilingNoDecryptPerRequest.
//
// The benchmark wires a SecretLookup closure that mimics iam.NewSecretLookup
// — a pure map read returning the cached plaintext — and then drives
// Verifier.Verify against a real sigv4-signed GET. No IAM Store is needed
// here; the Verifier's SecretLookup field is the integration point.
func BenchmarkVerify_NoDecryptPerRequest(b *testing.B) {
	const accessKey = "AKIDR2BENCH"
	const secretKey = "supersecret-bench"

	v := &Verifier{
		SecretLookup: func(ak string) (string, bool) {
			if ak == accessKey {
				return secretKey, true
			}
			return "", false
		},
	}

	// Build the signed request once outside the timed loop; the bench
	// measures the per-Verify cost, not signing.
	req, err := http.NewRequest(http.MethodGet, "http://localhost:9000/bench-bucket", nil)
	require.NoError(b, err)
	req.Host = "localhost:9000"
	SignRequest(req, accessKey, secretKey, "us-east-1")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ak, err := v.Verify(req)
		if err != nil {
			b.Fatalf("verify: %v", err)
		}
		if ak != accessKey {
			b.Fatalf("got ak %q, want %q", ak, accessKey)
		}
	}
}

// TestVerify_AllocCeilingNoDecryptPerRequest is the regression-guard CI
// check that pairs with the benchmark. It runs the benchmark briefly and
// asserts allocs/op stays under a ceiling chosen with headroom over the
// empirical post-R2 baseline. A future change that adds a per-request
// decrypt to the verify chain will trip this without anyone having to
// remember to run benchmarks manually.
//
// Ceiling rationale: a clean post-R2 run on this branch measured ~30
// allocs/op (canonical-request building, HMAC byte slices, header
// inspection). Lock at 60 — 2x headroom for noise + minor future shifts in
// the canonical-request builder, but tight enough to trip if Verify starts
// allocating a fresh decrypt buffer per call (which is roughly the cost of
// the AEAD output, easily >100 allocs/op of various sizes).
func TestVerify_AllocCeilingNoDecryptPerRequest(t *testing.T) {
	res := testing.Benchmark(BenchmarkVerify_NoDecryptPerRequest)
	allocsPerOp := res.AllocsPerOp()
	const ceiling = 60
	if allocsPerOp > int64(ceiling) {
		t.Fatalf("sigv4 Verify alloc'd %d/op (>%d ceiling) — did Verify start calling UnwrapSecret per request?", allocsPerOp, ceiling)
	}
	t.Logf("sigv4 Verify: %d allocs/op (ceiling %d)", allocsPerOp, ceiling)
}
