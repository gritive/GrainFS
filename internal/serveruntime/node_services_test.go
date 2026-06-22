package serveruntime

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeServicesProtocolStatusReportsFailedListenerAsDisabled(t *testing.T) {
	// Bind on all interfaces so the NFS4 listener (which also binds 0.0.0.0)
	// fails with EADDRINUSE.
	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	svc := StartNodeServices(context.Background(), nil, port, "", 0, "")
	t.Cleanup(svc.Close)

	status := svc.ProtocolStatus(Config{NFS4Port: port})
	require.False(t, status.NFS4.Enabled)
	require.Equal(t, port, status.NFS4.Port)
	require.Contains(t, status.NFS4.Warning, "start failed")
}
