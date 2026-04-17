package main

import (
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/migration"
)

func init() {
	migrateInjectCmd.Flags().String("src", "", "source S3-compatible endpoint (required)")
	migrateInjectCmd.Flags().String("dst", "", "destination S3-compatible endpoint (required)")
	migrateInjectCmd.Flags().String("src-access-key", "", "access key for source S3 endpoint")
	migrateInjectCmd.Flags().String("src-secret-key", "", "secret key for source S3 endpoint")
	migrateInjectCmd.Flags().String("dst-access-key", "", "access key for destination S3 endpoint")
	migrateInjectCmd.Flags().String("dst-secret-key", "", "secret key for destination S3 endpoint")
	migrateInjectCmd.Flags().Bool("skip-existing", false, "skip objects that already exist in the destination")
	_ = migrateInjectCmd.MarkFlagRequired("src")
	_ = migrateInjectCmd.MarkFlagRequired("dst")

	migrateCmd.AddCommand(migrateInjectCmd)
	rootCmd.AddCommand(migrateCmd)
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migration utilities",
}

var migrateInjectCmd = &cobra.Command{
	Use:   "inject",
	Short: "Copy all objects from a source S3 endpoint into a destination GrainFS",
	RunE:  runMigrateInject,
}

func runMigrateInject(cmd *cobra.Command, args []string) error {
	srcEndpoint, _ := cmd.Flags().GetString("src")
	dstEndpoint, _ := cmd.Flags().GetString("dst")
	srcAccessKey, _ := cmd.Flags().GetString("src-access-key")
	srcSecretKey, _ := cmd.Flags().GetString("src-secret-key")
	dstAccessKey, _ := cmd.Flags().GetString("dst-access-key")
	dstSecretKey, _ := cmd.Flags().GetString("dst-secret-key")
	skipExisting, _ := cmd.Flags().GetBool("skip-existing")

	src := migration.NewS3Source(srcEndpoint, srcAccessKey, srcSecretKey)
	dst := migration.NewS3Destination(dstEndpoint, dstAccessKey, dstSecretKey)

	var opts []migration.Option
	if skipExisting {
		opts = append(opts, migration.WithSkipExisting(true))
	}

	inj := migration.NewInjector(src, dst, opts...)
	stats, err := inj.Run()
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	slog.Info("migration complete",
		"copied", stats.Copied,
		"skipped", stats.Skipped,
		"errors", stats.Errors,
	)
	fmt.Printf("copied: %d  skipped: %d  errors: %d\n", stats.Copied, stats.Skipped, stats.Errors)
	return nil
}
