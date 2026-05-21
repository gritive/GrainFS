package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/nbd"
	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/p9server"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

// NodeServices tracks started services so they can be cleanly torn down
// on shutdown. Zero value is valid — fields are populated as services
// start.
type NodeServices struct {
	nfs4Srv *nfs4server.Server
	nbdSrv  *nbd.Server
	p9Srv   *p9server.Server
	nfs4Err error
	nbdErr  error
	p9Err   error
}

// Close shuts down any started services. Safe to call on the zero value.
func (n *NodeServices) Close() {
	if n.p9Srv != nil {
		if err := n.p9Srv.Close(); err != nil {
			log.Warn().Err(err).Msg("9p server close error")
		}
	}
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

// ProtocolStatus reports the protocols that actually started. Configured
// protocols that failed to bind remain disabled with a warning.
func (n *NodeServices) ProtocolStatus(cfg Config) adminapi.StorageProtocolStatusResp {
	resp := storageProtocolStatusFromConfig(cfg)
	resp.NFS4.Enabled = n.nfs4Srv != nil
	resp.NBD.Enabled = n.nbdSrv != nil
	resp.P9.Enabled = n.p9Srv != nil
	if cfg.NFS4Port > 0 && n.nfs4Srv == nil && n.nfs4Err != nil {
		resp.NFS4.Warning = "start failed: " + n.nfs4Err.Error()
	}
	if cfg.NBDPort > 0 && n.nbdSrv == nil && n.nbdErr != nil {
		resp.NBD.Warning = "start failed: " + n.nbdErr.Error()
	}
	if cfg.P9Port > 0 && n.p9Srv == nil && n.p9Err != nil {
		resp.P9.Warning = "start failed: " + n.p9Err.Error()
	}
	return resp
}

func (n *NodeServices) SetNFSExports(src *nfsexport.ExportService) {
	if n.nfs4Srv != nil {
		n.nfs4Srv.SetExportSource(src)
	}
	if n.p9Srv != nil && src != nil {
		n.p9Srv.SetExportStore(src)
	}
}

// NodeServicesIAMConfig carries optional IAM gate dependencies for NFS/9P
// servers (NFS§B T8 + T12). Nil fields disable the corresponding gate
// (backward compat).
//
// CfgStore is read per-op on the NFS/9P data path to enforce the Phase 0 →
// Phase 2 anon flip (T12 / §9 T73 parity): active anon-bound sessions are
// rejected on the next op after iam.anon-enabled is flipped false.
type NodeServicesIAMConfig struct {
	MountSAStore *mountsastore.Store
	Authorizer   *s3auth.Authorizer
	CfgStore     *config.Store
}

// StartNodeServices spawns NFSv4, NBD, and 9P servers if their respective ports
// are > 0. Returns the handle for shutdown. ri is an optional ReadIndexer
// for linearizable NBD reads (nil = no gate). iam is optional; nil = no IAM gate.
func StartNodeServices(ctx context.Context, backend storage.Backend,
	volMgr *volume.Manager, nfs4Port, nbdPort int, p9Bind string, p9Port int, ri nbd.ReadIndexer,
	iam ...*NodeServicesIAMConfig,
) *NodeServices {
	svc := &NodeServices{}

	var iamCfg *NodeServicesIAMConfig
	if len(iam) > 0 {
		iamCfg = iam[0]
	}

	if nfs4Port > 0 {
		nfs4Addr := fmt.Sprintf(":%d", nfs4Port)
		ln, err := net.Listen("tcp", nfs4Addr)
		if err != nil {
			svc.nfs4Err = fmt.Errorf("nfs4 listen: %w", err)
			log.Error().Err(svc.nfs4Err).Msg("nfs4 server start failed")
		} else {
			var nfs4Opts []nfs4server.ServerOption
			if iamCfg != nil && iamCfg.MountSAStore != nil {
				nfs4Opts = append(nfs4Opts, nfs4server.WithMountSAStore(iamCfg.MountSAStore))
			}
			if iamCfg != nil && iamCfg.Authorizer != nil {
				nfs4Opts = append(nfs4Opts, nfs4server.WithNFS4Authorizer(iamCfg.Authorizer))
			}
			if iamCfg != nil && iamCfg.CfgStore != nil {
				nfs4Opts = append(nfs4Opts, nfs4server.WithConfigReader(iamCfg.CfgStore))
			}
			svc.nfs4Srv = nfs4server.NewServer(backend, nfs4Opts...)
			go func() {
				if err := svc.nfs4Srv.Serve(ln); err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					log.Error().Err(err).Msg("nfs4 server error")
				}
			}()
		}
	}

	if nbdPort > 0 {
		const defaultVolName = "default"
		nbdSrv, err := startNBDServer(volMgr, defaultVolName, nbdPort, ri)
		if err != nil {
			svc.nbdErr = err
			log.Error().Err(err).Msg("nbd server start failed")
		} else {
			svc.nbdSrv = nbdSrv
		}
	}

	if p9Port > 0 {
		if p9Bind == "" {
			p9Bind = "127.0.0.1"
		}
		addr := net.JoinHostPort(p9Bind, fmt.Sprintf("%d", p9Port))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			svc.p9Err = fmt.Errorf("9p listen %s: %w", addr, err)
			log.Error().Err(svc.p9Err).Msg("9p server start failed")
		} else {
			var p9Opts []p9server.ServerOption
			if iamCfg != nil && iamCfg.MountSAStore != nil {
				p9Opts = append(p9Opts, p9server.WithMountSAStore(iamCfg.MountSAStore))
			}
			if iamCfg != nil && iamCfg.Authorizer != nil {
				p9Opts = append(p9Opts, p9server.WithAuthorizer(iamCfg.Authorizer))
			}
			if iamCfg != nil && iamCfg.CfgStore != nil {
				p9Opts = append(p9Opts, p9server.WithConfigReader(iamCfg.CfgStore))
			}
			svc.p9Srv = p9server.NewServer(backend, p9Opts...)
			go func() {
				if err := svc.p9Srv.Serve(ctx, ln); err != nil && ctx.Err() == nil {
					log.Error().Err(err).Msg("9p server error")
				}
			}()
		}
	}

	return svc
}

func startNBDServer(mgr *volume.Manager, volName string, port int, ri nbd.ReadIndexer) (*nbd.Server, error) {
	srv := nbd.NewServer(mgr, volName)
	if ri != nil {
		srv.SetReadIndexer(ri)
	}
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("nbd listen: %w", err)
	}
	log.Info().Str("component", "nbd").Str("addr", ln.Addr().String()).Str("volume", volName).Msg("nbd server started")
	go func() {
		if err := srv.Serve(ln); err != nil {
			log.Error().Err(err).Msg("nbd server error")
		}
	}()
	return srv, nil
}
