package serveruntime

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeServicesProtocolStatusReportsFailedListenerAsDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	svc := StartNodeServices(context.Background(), nil, nil, 0, 0, "127.0.0.1", port, nil)
	t.Cleanup(svc.Close)

	status := svc.ProtocolStatus(Config{P9Bind: "127.0.0.1", P9Port: port})
	require.False(t, status.P9.Enabled)
	require.Equal(t, "127.0.0.1", status.P9.Bind)
	require.Equal(t, port, status.P9.Port)
	require.Contains(t, status.P9.Warning, "start failed")
}
