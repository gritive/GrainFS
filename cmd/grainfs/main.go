package main

import (
	"os"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
		var zlvl zerolog.Level
		switch lvlStr {
		case "debug":
			zlvl = zerolog.DebugLevel
			hlog.SetLevel(hlog.LevelDebug)
		case "warn", "warning":
			zlvl = zerolog.WarnLevel
			hlog.SetLevel(hlog.LevelWarn)
		case "error":
			zlvl = zerolog.ErrorLevel
			hlog.SetLevel(hlog.LevelError)
		default:
			zlvl = zerolog.InfoLevel
			hlog.SetLevel(hlog.LevelInfo)
		}
		zerolog.SetGlobalLevel(zlvl)
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
		return nil
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
