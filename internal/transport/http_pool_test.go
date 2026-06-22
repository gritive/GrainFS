package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/gritive/GrainFS/internal/metrics"
)

// TestHTTPClient_ExplicitPoolOptions pins the client-side connection-pool
// contract: the cached Hertz client is built with EXPLICIT keep-alive pool
// options (not opaque defaults) so the pool sizing is an intentional, observable
// contract. RED before httpClient() passes WithMaxConnsPerHost/
// WithMaxIdleConnDuration/WithKeepAlive — GetOptions().MaxConnsPerHost is the
// struct zero (0) for a client built without the explicit option.
func TestHTTPClient_ExplicitPoolOptions(t *testing.T) {
	tr := MustNewHTTPTransport("pool-psk")
	t.Cleanup(func() { tr.Close() })

	c, err := tr.httpClient()
	if err != nil {
		t.Fatalf("httpClient: %v", err)
	}
	opts := c.GetOptions()

	tests := []struct {
		name string
		ok   bool
	}{
		{"MaxConnsPerHost set explicitly (>=512, no regression below the implicit default)", opts.MaxConnsPerHost >= 512},
		{"MaxIdleConnDuration is a non-zero explicit bound", opts.MaxIdleConnDuration > 0},
		{"KeepAlive explicitly enabled (the pool + httpRetryIf depend on it)", opts.KeepAlive},
	}
	for _, tc := range tests {
		if !tc.ok {
			t.Errorf("%s: assertion failed (MaxConnsPerHost=%d MaxIdleConnDuration=%s KeepAlive=%t)",
				tc.name, opts.MaxConnsPerHost, opts.MaxIdleConnDuration, opts.KeepAlive)
		}
	}
}

// TestHTTPClient_ConnectionReuse proves the keep-alive pool actually reuses
// connections: N sequential CallBuffered to ONE address must NOT each open a
// fresh wire conn. Cold dials are counted at the single dial seam
// (httpFreshDialer.DialConnection) via an injected counter. Without keep-alive
// every Do() would dial → counter≈N; with the pool one conn is reused → counter
// stays small (1 data conn + the pingReady probe conn), well below N.
//
// Mutation-verify: flip httpClient()'s WithKeepAlive(true) → WithKeepAlive(false)
// and this test goes RED (counter climbs to N), confirming it actually exercises
// pooling.
func TestHTTPClient_ConnectionReuse(t *testing.T) {
	srv := MustNewHTTPTransport("reuse-pool-psk")
	cli := MustNewHTTPTransport("reuse-pool-psk")
	t.Cleanup(func() { srv.Close(); cli.Close() })

	var dials atomic.Int64
	cli.dialCounter = &dials // test seam: incremented per cold dial

	srv.RegisterBufferedRoute(RouteShardRPC, func(payload []byte) ([]byte, error) {
		return append([]byte("echo:"), payload...), nil
	})
	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	const n = 20
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if _, err := cli.CallBuffered(ctx, addr, RouteShardRPC, []byte("hi")); err != nil {
			t.Fatalf("CallBuffered #%d: %v", i, err)
		}
	}

	got := dials.Load()
	if got >= n {
		t.Fatalf("dialed %d connections for %d sequential RPCs — pool is NOT reusing connections", got, n)
	}
	t.Logf("%d sequential RPCs drove %d cold dials (pool reuse)", n, got)
}

// TestHTTPClient_DialMetric_IncrementsOnColdDialNotOnReuse asserts the pool
// observability metric: the cold-dial counter rises on a genuine pool-miss dial
// and does NOT rise on a pool-reused RPC.
func TestHTTPClient_DialMetric_IncrementsOnColdDialNotOnReuse(t *testing.T) {
	srv := MustNewHTTPTransport("dial-metric-psk")
	cli := MustNewHTTPTransport("dial-metric-psk")
	t.Cleanup(func() { srv.Close(); cli.Close() })

	srv.RegisterBufferedRoute(RouteShardRPC, func(payload []byte) ([]byte, error) {
		return []byte("ok"), nil
	})
	addr := listenHTTP(t, srv)

	before := testutil.ToFloat64(metrics.TransportClientDialsTotal)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("server not ready: %v", err)
	}
	afterColdDial := testutil.ToFloat64(metrics.TransportClientDialsTotal)
	if afterColdDial <= before {
		t.Fatalf("cold dial did not increment TransportClientDialsTotal (before=%v after=%v)", before, afterColdDial)
	}

	// A reused RPC on the warmed pool must NOT add another cold dial.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.CallBuffered(ctx, addr, RouteShardRPC, []byte("x")); err != nil {
		t.Fatalf("CallBuffered: %v", err)
	}
	afterReuse := testutil.ToFloat64(metrics.TransportClientDialsTotal)
	if afterReuse != afterColdDial {
		t.Fatalf("pool-reused RPC incremented the cold-dial counter (after-cold=%v after-reuse=%v) — reuse must not dial",
			afterColdDial, afterReuse)
	}
}
