package serveruntime

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// TestLogClusterConfigLoaded_EmitsStructuredEvent verifies the boot-time
// "cluster config loaded" event carries the expected key set and never
// leaks the wrapped secret bytes.
func TestLogClusterConfigLoaded_EmitsStructuredEvent(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).With().Timestamp().Logger()
	defer func() { log.Logger = prev }()

	logClusterConfigLoaded(cluster.NewClusterConfig())

	s := buf.String()
	require.Contains(t, s, `"event":"cluster_config_loaded"`)
	require.Contains(t, s, `"rev":0`)
	require.Contains(t, s, `"balancer-enabled":`)
	require.Contains(t, s, `"balancer-imbalance-trigger-pct":`)
	require.Contains(t, s, `"balancer-imbalance-stop-pct":`)
	require.Contains(t, s, `"balancer-migration-rate":`)
	require.Contains(t, s, `"balancer-leader-tenure-min":`)
	require.Contains(t, s, `"balancer-warmup-timeout":`)
	require.Contains(t, s, `"balancer-cb-threshold":`)
	require.Contains(t, s, `"balancer-migration-max-retries":`)
	require.Contains(t, s, `"balancer-migration-pending-ttl":`)
	require.Contains(t, s, `"balancer-gossip-interval":`)
	require.Contains(t, s, `"alert-webhook":`)
	require.Contains(t, s, `"alert-webhook-secret-set":false`)
	require.Contains(t, s, `"disk-warn-threshold":`)
	require.Contains(t, s, `"disk-critical-threshold":`)
	require.Contains(t, s, `"message":"cluster config loaded"`)
}
