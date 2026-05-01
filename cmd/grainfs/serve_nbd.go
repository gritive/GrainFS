package main

import (
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/volume"
)

func startNBDServer(mgr *volume.Manager, volName string, port int, ri nbd.ReadIndexer) (*nbd.Server, error) {
	srv := nbd.NewServer(mgr, volName)
	if ri != nil {
		srv.SetReadIndexer(ri)
	}
	go func() {
		addr := fmt.Sprintf(":%d", port)
		if err := srv.ListenAndServe(addr); err != nil {
			log.Error().Err(err).Msg("nbd server error")
		}
	}()
	return srv, nil
}
