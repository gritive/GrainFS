//go:build linux

package main

import (
	"fmt"
	"log/slog"

	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/volume"
)

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
