package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/storage"
)

var version = "dev"

var rootCmd = &cobra.Command{
	Use:     "grainfs",
	Short:   "GrainFS - Lightweight S3-compatible object storage",
	Version: version,
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the S3-compatible storage server",
	RunE: func(cmd *cobra.Command, args []string) error {
		dataDir, _ := cmd.Flags().GetString("data")
		port, _ := cmd.Flags().GetInt("port")

		backend, err := storage.NewLocalBackend(dataDir)
		if err != nil {
			return fmt.Errorf("failed to initialize storage: %w", err)
		}

		addr := fmt.Sprintf(":%d", port)
		log.Printf("GrainFS %s listening on %s (data: %s)", version, addr, dataDir)

		srv := server.New(addr, backend)

		// Graceful shutdown on SIGINT/SIGTERM
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		go srv.Run()

		<-ctx.Done()
		log.Println("Shutting down...")
		backend.Close()
		log.Println("GrainFS stopped.")
		return nil
	},
}

func init() {
	serveCmd.Flags().StringP("data", "d", "./data", "data directory")
	serveCmd.Flags().IntP("port", "p", 9000, "listen port")
	rootCmd.AddCommand(serveCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
