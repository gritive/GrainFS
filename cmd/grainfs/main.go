package main

import (
	"log/slog"
	"os"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/spf13/cobra"
)

var version = "dev"

var rootCmd = &cobra.Command{
	Use:     "grainfs",
	Short:   "GrainFS - Lightweight S3-compatible object storage",
	Version: version,
}

func init() {
	rootCmd.PersistentFlags().String("log-level", "info", "log level: debug, info, warn, error")
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		lvlStr, _ := rootCmd.PersistentFlags().GetString("log-level")
		var slogLvl slog.Level
		switch lvlStr {
		case "debug":
			slogLvl = slog.LevelDebug
			hlog.SetLevel(hlog.LevelDebug)
		case "warn", "warning":
			slogLvl = slog.LevelWarn
			hlog.SetLevel(hlog.LevelWarn)
		case "error":
			slogLvl = slog.LevelError
			hlog.SetLevel(hlog.LevelError)
		default:
			slogLvl = slog.LevelInfo
			hlog.SetLevel(hlog.LevelInfo)
		}
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slogLvl})))
		return nil
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
