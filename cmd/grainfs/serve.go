package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"

	"crypto/rand"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/erasure"
	"github.com/gritive/GrainFS/internal/nfsserver"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

func init() {
	serveCmd.Flags().StringP("data", "d", "./data", "data directory")
	serveCmd.Flags().IntP("port", "p", 9000, "listen port")
	serveCmd.Flags().String("node-id", "", "unique node ID (auto-generated if omitted)")
	serveCmd.Flags().String("raft-addr", "", "Raft listen address (required when --peers is set)")
	serveCmd.Flags().String("peers", "", "comma-separated list of peer Raft addresses (enables cluster mode)")
	serveCmd.Flags().Bool("ec", true, "enable erasure coding (Reed-Solomon 4+2, use --ec=false to disable)")
	serveCmd.Flags().Int("ec-data", erasure.DefaultDataShards, "number of data shards for erasure coding")
	serveCmd.Flags().Int("ec-parity", erasure.DefaultParityShards, "number of parity shards for erasure coding")
	serveCmd.Flags().String("access-key", "", "S3 access key for authentication (enables auth when set)")
	serveCmd.Flags().String("secret-key", "", "S3 secret key for authentication")
	serveCmd.Flags().String("encryption-key-file", "", "path to 32-byte encryption key file (auto-generated if omitted)")
	serveCmd.Flags().Bool("no-encryption", false, "disable at-rest encryption")
	serveCmd.Flags().Int("nfs-port", 9002, "NFS server port (0 = disabled, volumes managed via REST API)")
	rootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the S3-compatible storage server",
	RunE:  runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data")
	port, _ := cmd.Flags().GetInt("port")
	peersStr, _ := cmd.Flags().GetString("peers")

	addr := fmt.Sprintf(":%d", port)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ecEnabled, _ := cmd.Flags().GetBool("ec")
	ecData, _ := cmd.Flags().GetInt("ec-data")
	ecParity, _ := cmd.Flags().GetInt("ec-parity")

	var authOpts []server.Option
	accessKey, _ := cmd.Flags().GetString("access-key")
	secretKey, _ := cmd.Flags().GetString("secret-key")
	if accessKey != "" && secretKey != "" {
		authOpts = append(authOpts, server.WithAuth([]s3auth.Credentials{
			{AccessKey: accessKey, SecretKey: secretKey},
		}))
	}

	var ecOpts []erasure.ECOption
	noEncryption, _ := cmd.Flags().GetBool("no-encryption")
	if !noEncryption {
		encKeyFile, _ := cmd.Flags().GetString("encryption-key-file")
		enc, err := loadOrCreateEncryptionKey(encKeyFile, dataDir)
		if err != nil {
			return fmt.Errorf("encryption setup: %w", err)
		}
		ecOpts = append(ecOpts, erasure.WithEncryption(enc))
	}

	nfsPort, _ := cmd.Flags().GetInt("nfs-port")

	if peersStr == "" {
		var backend storage.Backend
		var err error
		if ecEnabled {
			backend, err = erasure.NewECBackend(dataDir, ecData, ecParity, ecOpts...)
		} else {
			backend, err = storage.NewLocalBackend(dataDir)
		}
		if err != nil {
			return fmt.Errorf("failed to initialize storage: %w", err)
		}
		mode := "solo"
		if ecEnabled {
			mode = "solo-ec"
		}
		return runSoloWithNFS(ctx, addr, dataDir, mode, backend, authOpts, nfsPort)
	}

	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	return runCluster(ctx, addr, dataDir, nodeID, raftAddr, peersStr)
}

func runSoloWithNFS(ctx context.Context, addr, dataDir, mode string, backend storage.Backend, opts []server.Option, nfsPort int) error {
	slog.Info("server started", "component", "server", "mode", mode, "version", version, "addr", addr, "data", dataDir)

	srv := server.New(addr, backend, opts...)
	go srv.Run()

	// Start NFS server if requested
	if nfsPort > 0 {
		const defaultVolName = "default"
		const defaultVolSize = 1024 * 1024 * 1024 // 1G

		mgr := volume.NewManager(backend)
		// Ensure a default volume exists for NFS
		if _, err := mgr.Get(defaultVolName); err != nil {
			if _, err := mgr.Create(defaultVolName, defaultVolSize); err != nil {
				slog.Warn("default nfs volume create failed (may already exist)", "error", err)
			}
		}

		nfsSrv := nfsserver.NewServer(backend, defaultVolName)
		go func() {
			nfsAddr := fmt.Sprintf(":%d", nfsPort)
			if err := nfsSrv.ListenAndServe(nfsAddr); err != nil {
				slog.Error("nfs server error", "error", err)
			}
		}()
	}

	<-ctx.Done()
	slog.Info("shutting down", "component", "server")
	if closer, ok := backend.(interface{ Close() error }); ok {
		closer.Close()
	}
	slog.Info("server stopped", "component", "server")
	return nil
}

func runCluster(ctx context.Context, addr, dataDir, nodeID, raftAddr, peersStr string) error {
	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
		slog.Info("auto-generated node ID", "component", "server", "node_id", nodeID)
	}
	if raftAddr == "" {
		return fmt.Errorf("--raft-addr is required when --peers is set")
	}

	peers := strings.Split(peersStr, ",")

	metaDir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db: %w", err)
	}
	defer db.Close()

	raftDir := filepath.Join(dataDir, "raft")
	logStore, err := raft.NewBadgerLogStore(raftDir)
	if err != nil {
		return fmt.Errorf("open raft store: %w", err)
	}
	defer logStore.Close()

	cfg := raft.DefaultConfig(nodeID, peers)
	node := raft.NewNode(cfg, logStore)

	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("peer %s not reachable (stub transport)", peer)
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("peer %s not reachable (stub transport)", peer)
		},
	)
	node.Start()
	defer node.Stop()

	backend, err := cluster.NewDistributedBackend(dataDir, db, node)
	if err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	slog.Info("server started", "component", "server", "mode", "cluster", "version", version,
		"node_id", nodeID, "raft_addr", raftAddr, "peers", peers, "addr", addr, "data", dataDir)

	srv := server.New(addr, backend)
	go srv.Run()

	<-ctx.Done()
	slog.Info("shutting down", "component", "server")
	close(stopApply)
	slog.Info("server stopped", "component", "server")
	return nil
}

// loadOrCreateEncryptionKey loads a key from file or auto-generates one in the data directory.
func loadOrCreateEncryptionKey(keyFile, dataDir string) (*encrypt.Encryptor, error) {
	if keyFile == "" {
		keyFile = filepath.Join(dataDir, "encryption.key")
	}

	keyData, err := os.ReadFile(keyFile)
	if err == nil {
		slog.Info("at-rest encryption enabled", "component", "server", "key_file", keyFile)
		return encrypt.NewEncryptor(keyData)
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Auto-generate a new key
	if err := os.MkdirAll(filepath.Dir(keyFile), 0o755); err != nil {
		return nil, fmt.Errorf("create key dir: %w", err)
	}
	keyData = make([]byte, 32)
	if _, err := rand.Read(keyData); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	if err := os.WriteFile(keyFile, keyData, 0o600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}

	slog.Info("at-rest encryption enabled (auto-generated key)", "component", "server", "key_file", keyFile)
	return encrypt.NewEncryptor(keyData)
}
