package main

// node_services.go wires the universal "node services" (NFSv4, NBD)
// onto any storage.Backend. Both the legacy local dispatch and cluster mode
// call startNodeServices so NFSv4/NBD are available regardless of which
// storage topology the operator picked.

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// nodeServices tracks started services so they can be cleanly torn down on
// shutdown. Zero value is valid — fields are populated as services start.
type nodeServices struct {
	nfs4Srv *nfs4server.Server
	nbdSrv  *nbd.Server
}

// Close shuts down any started services. Safe to call on the zero value.
func (n *nodeServices) Close() {
	if n.nbdSrv != nil {
		if err := n.nbdSrv.Close(); err != nil {
			log.Warn().Err(err).Msg("nbd server close error")
		}
	}
	// nfs4Srv exposes no Close; relies on context cancellation.
}

// startNodeServices spawns NFSv4 and NBD servers if their respective
// ports are > 0. Returns the handle for shutdown and nil on success.
//
// Args:
//   - backend: the storage backend NFSv4 should mount.
//   - volMgr: shared volume.Manager (may have dedup enabled); used by NBD.
//   - nfs4Port, nbdPort: 0 disables the service.
//   - nbdVolumeSize: default volume size when the NBD worker auto-creates it.
//   - ri: optional ReadIndexer for linearizable NBD reads; nil = no gate.
func startNodeServices(ctx context.Context, cmd *cobra.Command, backend storage.Backend,
	volMgr *volume.Manager, nfs4Port, nbdPort int, nbdVolumeSize int64, ri nbd.ReadIndexer,
) *nodeServices {
	svc := &nodeServices{}

	if nfs4Port > 0 {
		svc.nfs4Srv = nfs4server.NewServer(backend)
		go func() {
			nfs4Addr := fmt.Sprintf(":%d", nfs4Port)
			if err := svc.nfs4Srv.ListenAndServe(nfs4Addr); err != nil {
				log.Error().Err(err).Msg("nfs4 server error")
			}
		}()
	}

	if nbdPort > 0 {
		const defaultVolName = "default"
		if _, err := volMgr.Get(defaultVolName); err != nil {
			if _, err := volMgr.Create(defaultVolName, nbdVolumeSize); err != nil {
				log.Warn().Err(err).Msg("default nbd volume create failed")
			}
		}
		nbdSrv, err := startNBDServer(volMgr, defaultVolName, nbdPort, ri)
		if err != nil {
			log.Error().Err(err).Msg("nbd server start failed")
		} else {
			svc.nbdSrv = nbdSrv
		}
	}

	return svc
}
