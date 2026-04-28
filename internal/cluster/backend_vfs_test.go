package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetVFSFixedVersionEnabled_default(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.True(t, b.VFSFixedVersionEnabled(),
		"default must be true so disk-amplification fix is active out of the box")
}

func TestSetVFSFixedVersionEnabled_toggle(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetVFSFixedVersionEnabled(false)
	require.False(t, b.VFSFixedVersionEnabled())
	b.SetVFSFixedVersionEnabled(true)
	require.True(t, b.VFSFixedVersionEnabled())
}
