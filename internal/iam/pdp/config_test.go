package pdp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	t.Run("default empty is disabled", func(t *testing.T) {
		c, err := ParseConfig([]byte(`{}`))
		require.NoError(t, err)
		require.False(t, c.Enabled)
		require.Equal(t, 2*time.Second, c.Timeout)
		require.Equal(t, FailureClosed, c.FailurePolicy)
	})
	t.Run("valid enabled unix endpoint", func(t *testing.T) {
		c, err := ParseConfig([]byte(`{"enabled":true,"endpoint":"unix:///run/grainfs/pdp.sock","timeout":"3s","failure_policy":"open"}`))
		require.NoError(t, err)
		require.True(t, c.Enabled)
		require.Equal(t, "/run/grainfs/pdp.sock", c.SocketPath)
		require.Equal(t, 3*time.Second, c.Timeout)
		require.Equal(t, FailureOpen, c.FailurePolicy)
	})
	t.Run("enabled rejects missing endpoint", func(t *testing.T) {
		_, err := ParseConfig([]byte(`{"enabled":true}`))
		require.Error(t, err)
	})
	t.Run("enabled rejects non-unix scheme", func(t *testing.T) {
		_, err := ParseConfig([]byte(`{"enabled":true,"endpoint":"https://pdp.example.com/authorize"}`))
		require.Error(t, err)
	})
	t.Run("rejects bad timeout and over-cap", func(t *testing.T) {
		_, err := ParseConfig([]byte(`{"enabled":true,"endpoint":"unix:///s.sock","timeout":"nope"}`))
		require.Error(t, err)
		_, err = ParseConfig([]byte(`{"enabled":true,"endpoint":"unix:///s.sock","timeout":"11s"}`))
		require.Error(t, err)
	})
	t.Run("rejects bad failure_policy", func(t *testing.T) {
		_, err := ParseConfig([]byte(`{"enabled":true,"endpoint":"unix:///s.sock","failure_policy":"maybe"}`))
		require.Error(t, err)
	})
	t.Run("rejects relative unix socket path", func(t *testing.T) {
		_, err := ParseConfig([]byte(`{"enabled":true,"endpoint":"unix://relative.sock"}`))
		require.Error(t, err)
	})
	t.Run("rejects non-positive timeout", func(t *testing.T) {
		_, err := ParseConfig([]byte(`{"enabled":true,"endpoint":"unix:///s.sock","timeout":"0s"}`))
		require.Error(t, err)
		_, err = ParseConfig([]byte(`{"enabled":true,"endpoint":"unix:///s.sock","timeout":"-1s"}`))
		require.Error(t, err)
	})
	t.Run("empty bytes parses as disabled", func(t *testing.T) {
		c, err := ParseConfig([]byte(""))
		require.NoError(t, err)
		require.False(t, c.Enabled)
	})
	t.Run("disabled with empty endpoint still parses (disabled = inert)", func(t *testing.T) {
		c, err := ParseConfig([]byte(`{"enabled":false,"endpoint":""}`))
		require.NoError(t, err)
		require.False(t, c.Enabled)
	})
}
