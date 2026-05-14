package serveruntime

import (
	"errors"
	"fmt"
	"net"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// NodeServices tracks started services so they can be cleanly torn down
// on shutdown. Zero value is valid — fields are populated as services
// start.
type NodeServices struct {
	nfs4Srv *nfs4server.Server
	nbdSrv  *nbd.Server
}

// Close shuts down any started services. Safe to call on the zero value.
func (n *NodeServices) Close() {
	if n.nbdSrv != nil {
		if err := n.nbdSrv.Close(); err != nil {
			log.Warn().Err(err).Msg("nbd server close error")
		}
	}
	if n.nfs4Srv != nil {
		if err := n.nfs4Srv.Close(); err != nil {
			log.Warn().Err(err).Msg("nfs4 server close error")
		}
	}
}

// NFS4 returns the started NFSv4 server, or nil when NFS4 was not enabled
// (port 0). Exposed so the runtime can register the server's cache
// invalidator with the cluster registry after StartNodeServices returns.
func (n *NodeServices) NFS4() *nfs4server.Server { return n.nfs4Srv }

func (n *NodeServices) SetNFSExports(src *nfsexport.ExportService) {
	if n.nfs4Srv != nil {
		n.nfs4Srv.SetExportSource(src)
	}
}

// StartNodeServices spawns NFSv4 and NBD servers if their respective ports
// are > 0. Returns the handle for shutdown. ri is an optional ReadIndexer
// for linearizable NBD reads (nil = no gate).
func StartNodeServices(backend storage.Backend,
	volMgr *volume.Manager, nfs4Port, nbdPort int, ri nbd.ReadIndexer,
) *NodeServices {
	svc := &NodeServices{}

	if nfs4Port > 0 {
		svc.nfs4Srv = nfs4server.NewServer(backend)
		go func() {
			nfs4Addr := fmt.Sprintf(":%d", nfs4Port)
			if err := svc.nfs4Srv.ListenAndServe(nfs4Addr); err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Error().Err(err).Msg("nfs4 server error")
			}
		}()
	}

	if nbdPort > 0 {
		const defaultVolName = "default"
		nbdSrv, err := startNBDServer(volMgr, defaultVolName, nbdPort, ri)
		if err != nil {
			log.Error().Err(err).Msg("nbd server start failed")
		} else {
			svc.nbdSrv = nbdSrv
		}
	}

	return svc
}

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
