package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageProtocolStatusFromConfig(t *testing.T) {
	resp := storageProtocolStatusFromConfig(Config{
		NFS4Port: 2049,
		NBDPort:  0,
		P9Bind:   "127.0.0.1",
		P9Port:   564,
	})

	require.True(t, resp.NFS4.Enabled)
	require.Equal(t, 2049, resp.NFS4.Port)
	require.False(t, resp.NBD.Enabled)
	require.True(t, resp.P9.Enabled)
	require.Equal(t, "127.0.0.1", resp.P9.Bind)
	require.Equal(t, 564, resp.P9.Port)
}
