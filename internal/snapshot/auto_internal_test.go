package snapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultIdleWhenDisabledBoundsHotReloadLatency(t *testing.T) {
	require.LessOrEqual(t, defaultIdleWhenDisabled, time.Second)
}
