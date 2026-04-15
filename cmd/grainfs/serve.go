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

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/erasure"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/storage"
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

	if peersStr == "" {
		if ecEnabled {
			return runSoloEC(ctx, addr, dataDir, ecData, ecParity)
		}
		return runSolo(ctx, addr, dataDir)
	}

	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	return runCluster(ctx, addr, dataDir, nodeID, raftAddr, peersStr)
}

func runSolo(ctx context.Context, addr, dataDir string) error {
	backend, err := storage.NewLocalBackend(dataDir)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	slog.Info("server started", "component", "server", "mode", "solo", "version", version, "addr", addr, "data", dataDir)

	srv := server.New(addr, backend)
	go srv.Run()

	<-ctx.Done()
	slog.Info("shutting down", "component", "server")
	backend.Close()
	slog.Info("server stopped", "component", "server")
	return nil
}

func runSoloEC(ctx context.Context, addr, dataDir string, ecData, ecParity int) error {
	backend, err := erasure.NewECBackend(dataDir, ecData, ecParity)
	if err != nil {
		return fmt.Errorf("failed to initialize EC storage: %w", err)
	}

	slog.Info("server started", "component", "server", "mode", "solo-ec",
		"version", version, "addr", addr, "data", dataDir,
		"ec_data", ecData, "ec_parity", ecParity)

	srv := server.New(addr, backend)
	go srv.Run()

	<-ctx.Done()
	slog.Info("shutting down", "component", "server")
	backend.Close()
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
