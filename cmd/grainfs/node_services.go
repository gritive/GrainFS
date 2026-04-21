package main

// node_services.go wires the universal "node services" (NFS v3, NFSv4, NBD)
// onto any storage.Backend. Both the legacy solo dispatch and cluster mode
// call startNodeServices so NFS/NFSv4/NBD are available regardless of which
// storage topology the operator picked.
//
// Services NOT covered here — scrubber, lifecycle worker, auto-snapshotter —
// are hard-coupled to internal/erasure.ECBackend or the WAL layer and live in
// the solo-specific path until a follow-up PR integrates them into cluster
// mode's DistributedBackend.

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/nfsserver"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/vfs"
	"github.com/gritive/GrainFS/internal/volume"
)

// nodeServices tracks started services so they can be cleanly torn down on
// shutdown. Zero value is valid — fields are populated as services start.
type nodeServices struct {
	nfsSrv  *nfsserver.Server
	nfs4Srv *nfs4server.Server
}

// Close shuts down any started services. Safe to call on the zero value.
func (n *nodeServices) Close() {
	if n.nfsSrv != nil {
		if err := n.nfsSrv.Close(); err != nil {
			slog.Warn("nfs server close error", "error", err)
		}
	}
	// nfs4Srv exposes no Close; relies on context cancellation.
}

// startNodeServices spawns NFS, NFSv4, and NBD servers if their respective
// ports are > 0. Returns the handle for shutdown and nil on success. Errors
// from individual services are logged and do not abort startup — a failed
// NFS listen is surfaced in logs but the HTTP/S3 path keeps serving.
//
// Args:
//   - backend: the storage backend NFS/NBD should mount (typically the same
//     backend the HTTP server uses; safe to pass a cached/wrapped backend).
//   - nfsPort, nfs4Port, nbdPort: 0 disables the service.
//   - nbdVolumeSize: default volume size when the NBD worker auto-creates it.
func startNodeServices(ctx context.Context, cmd *cobra.Command, backend storage.Backend,
	nfsPort, nfs4Port, nbdPort int, nbdVolumeSize int64,
) *nodeServices {
	svc := &nodeServices{}

	if nfsPort > 0 {
		fmt.Println("WARNING: NFS null auth enabled — all NFS access is unauthenticated")
		const defaultVolName = "default"
		const defaultVolSize = 1024 * 1024 * 1024 // 1G

		mgr := volume.NewManager(backend)
		if _, err := mgr.Get(defaultVolName); err != nil {
			if _, err := mgr.Create(defaultVolName, defaultVolSize); err != nil {
				slog.Warn("default nfs volume create failed (may already exist)", "error", err)
			}
		}

		svc.nfsSrv = nfsserver.NewServer(backend, defaultVolName, nil,
			vfs.WithStatCacheTTL(1e9),  // 1s
			vfs.WithDirCacheTTL(1e9),
		)
		go func() {
			nfsAddr := fmt.Sprintf(":%d", nfsPort)
			if err := svc.nfsSrv.ListenAndServe(nfsAddr); err != nil {
				slog.Error("nfs server error", "error", err)
			}
		}()
	}

	if nfs4Port > 0 {
		svc.nfs4Srv = nfs4server.NewServer(backend)
		go func() {
			// localhost only for AUTH_SYS security — NFSv4 with auth_sys on a
			// public interface would let any client impersonate any UID.
			nfs4Addr := fmt.Sprintf("127.0.0.1:%d", nfs4Port)
			if err := svc.nfs4Srv.ListenAndServe(nfs4Addr); err != nil {
				slog.Error("nfs4 server error", "error", err)
			}
		}()
	}

	if nbdPort > 0 {
		const defaultVolName = "default"
		mgr := volume.NewManager(backend)
		if _, err := mgr.Get(defaultVolName); err != nil {
			if _, err := mgr.Create(defaultVolName, nbdVolumeSize); err != nil {
				slog.Warn("default nbd volume create failed", "error", err)
			}
		}
		if _, err := startNBDServer(mgr, defaultVolName, nbdPort); err != nil {
			slog.Error("nbd server start failed", "error", err)
		}
	}

	// ctx is reserved for future per-service cancellation (some servers spawn
	// internal goroutines that currently only respect ListenAndServe return).
	_ = ctx
	_ = os.Getpid
	return svc
}
