package main

import (
	"fmt"
	"log/slog"

	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/volume"
)

// startNBDServer launches an NBD server bound to the given volume on the
// given port. The server itself is platform-agnostic; only the nbd-client
// tooling is Linux-specific.
func startNBDServer(mgr *volume.Manager, volName string, port int) (*nbd.Server, error) {
	srv := nbd.NewServer(mgr, volName)
	go func() {
		addr := fmt.Sprintf(":%d", port)
		if err := srv.ListenAndServe(addr); err != nil {
			slog.Error("nbd server error", "error", err)
		}
	}()
	return srv, nil
}
