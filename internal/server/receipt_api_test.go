package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	hertz "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestRegisterReceiptAPIDoesNotInstallRouteLevelAuth(t *testing.T) {
	port := freeLocalPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	h := hertz.Default(
		hertz.WithHostPorts(addr),
		hertz.WithExitWaitTime(time.Second),
	)
	srv := &Server{
		receiptAPI: receipt.NewAPI(nil, nil, nil, 0),
		verifier:   s3auth.NewCachingVerifier(s3auth.NewVerifier([]s3auth.Credentials{{AccessKey: "AK", SecretKey: "secret"}}), 4096, time.Minute),
	}
	srv.registerReceiptAPI(h)

	go h.Spin()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := http.Get("http://" + addr + "/api/receipts")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
