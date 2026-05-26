package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/volumeadmin"
)

// errSizeRequired is returned when --size is missing from create/resize.
var errSizeRequired = errors.New("--size required")

// invalidSizeErr wraps a parse error with the conventional "invalid --size:" prefix.
func invalidSizeErr(err error) error {
	return fmt.Errorf("invalid --size: %w", err)
}

// --- Cobra command tree ---

var volumeCmd = &cobra.Command{
	Use:   "volume",
	Short: "Volume management commands",
}

var volumeListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all volumes",
	Example: `  # Default tabular output
  grainfs volume list

  # JSON output (for scripting)
  grainfs volume list --format json

  # Raw byte values (no IEC suffixes)
  grainfs volume list --bytes`,
	RunE: runVolumeList,
}

var volumeCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new volume",
	Args:  cobra.ExactArgs(1),
	Example: `  # Create a 1 GiB volume (binary, 1024^3)
  grainfs volume create v1 --size 1Gi

  # Create a 1 GB volume (decimal, 1000^3)
  grainfs volume create v2 --size 1GB

  # Specify the size in bytes
  grainfs volume create v3 --size 1073741824`,
	RunE: runVolumeCreate,
}

var volumeInfoCmd = &cobra.Command{
	Use:     "info <name>",
	Short:   "Show volume metadata",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume info v1`,
	RunE:    runVolumeInfo,
}

var volumeStatCmd = &cobra.Command{
	Use:     "stat <name>",
	Short:   "Show volume usage and recent incidents",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume stat v1`,
	RunE:    runVolumeStat,
}

var volumeDeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Short:   "Delete a volume",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume delete v1`,
	RunE:    runVolumeDelete,
}

var volumeResizeCmd = &cobra.Command{
	Use:   "resize <name>",
	Short: "Resize a volume (grow only — shrink is rejected)",
	Args:  cobra.ExactArgs(1),
	Example: `  # Grow to 2 GiB
  grainfs volume resize v1 --size 2Gi`,
	RunE: runVolumeResize,
}

var volumeRecalculateCmd = &cobra.Command{
	Use:     "recalculate <name>",
	Short:   "Recalculate AllocatedBlocks by counting actual block objects",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume recalculate v1`,
	RunE:    runVolumeRecalculate,
}

var volumeWriteAtCmd = &cobra.Command{
	Use:     "write-at <name>",
	Short:   "Write bytes at a volume offset (debug/test helper)",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume write-at v1 --offset 0 --content 'hello'`,
	RunE:    runVolumeWriteAt,
}

var volumeReadAtCmd = &cobra.Command{
	Use:     "read-at <name>",
	Short:   "Read bytes from a volume offset (debug/test helper)",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume read-at v1 --offset 0 --length 32`,
	RunE:    runVolumeReadAt,
}

var volumeScrubCmd = &cobra.Command{
	Use:   "scrub <name>",
	Short: "Scrub a volume — detect and repair silent corruption",
	Long: `Scrub verifies every block of the named volume by reading the local copy
and comparing its MD5 against the stored ETag. Corrupt or missing blocks
are repaired by pulling a healthy peer replica.

--dry-run records detection without repair. --detach returns immediately
after triggering instead of following session progress.`,
	Example: `  grainfs volume scrub myvol
  grainfs volume scrub myvol --dry-run
  grainfs volume scrub myvol --detach`,
	Args: cobra.ExactArgs(1),
	RunE: runVolumeScrub,
}

var volumeScrubStatusCmd = &cobra.Command{
	Use:     "status <session_id>",
	Short:   "Show status of a scrub session",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume scrub status 0192e8a4-...`,
	RunE:    runVolumeScrubStatus,
}

var volumeScrubListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List active and recent scrub sessions",
	Example: `  grainfs volume scrub list`,
	RunE:    runVolumeScrubList,
}

var volumeScrubCancelCmd = &cobra.Command{
	Use:     "cancel <session_id>",
	Short:   "Cancel a running scrub session",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs volume scrub cancel 0192e8a4-...`,
	RunE:    runVolumeScrubCancel,
}

func init() {
	pf := volumeCmd.PersistentFlags()
	pf.String("format", "text", "Output format: text or json (json always uses raw bytes)")
	pf.Bool("bytes", false, "in text format, show sizes as raw byte counts instead of IEC suffixes")
	registerAdminEndpointFlag(volumeCmd)
	registerAdminTimeoutFlag(volumeCmd)

	volumeCreateCmd.Flags().String("size", "", `volume size — binary "1Gi"/"100Mi" (1024^n) or decimal "1GB"/"100MB" (1000^n); bare "1G"/"1M" rejected as ambiguous`)
	volumeResizeCmd.Flags().String("size", "", `new size (must be >= current); same units as create`)
	volumeWriteAtCmd.Flags().Int64("offset", 0, "byte offset")
	volumeWriteAtCmd.Flags().String("content", "", "bytes to write (string)")
	volumeReadAtCmd.Flags().Int64("offset", 0, "byte offset")
	volumeReadAtCmd.Flags().Int64("length", 0, "bytes to read (1..64MiB)")
	volumeScrubCmd.Flags().Bool("dry-run", false, "detect-only, do not repair")
	volumeScrubCmd.Flags().Bool("detach", false, "trigger and exit; do not follow session progress")

	volumeScrubCmd.AddCommand(volumeScrubStatusCmd, volumeScrubListCmd, volumeScrubCancelCmd)
	volumeCmd.AddCommand(
		volumeListCmd, volumeCreateCmd, volumeInfoCmd, volumeStatCmd,
		volumeDeleteCmd, volumeResizeCmd, volumeRecalculateCmd,
		volumeWriteAtCmd, volumeReadAtCmd,
		volumeScrubCmd,
	)
	rootCmd.AddCommand(volumeCmd)
}

// baseOptionsFromCmd reads the persistent flags every volume runner shares
// and packs them into a volumeadmin.BaseOptions. Format contract since
// v0.0.89: --format=text uses IEC suffixes by default, --bytes overrides
// text mode to show raw bytes; --format=json always uses raw bytes.
func baseOptionsFromCmd(cmd *cobra.Command) volumeadmin.BaseOptions {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	asJSON := jsonOut(cmd)
	bytesFlag, _ := cmd.Flags().GetBool("bytes")
	return volumeadmin.BaseOptions{
		Endpoint: endpoint,
		JSONOut:  asJSON,
		RawBytes: asJSON || bytesFlag,
		Timeout:  adminTimeoutFromCmd(cmd),
		Stdout:   cmd.OutOrStdout(),
		Stderr:   cmd.ErrOrStderr(),
	}
}

// --- Runners (thin: parse flags, build options, delegate) ---

func runVolumeList(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunList(cmd.Context(), volumeadmin.ListOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
	})
}

func runVolumeCreate(cmd *cobra.Command, args []string) error {
	sizeStr, _ := cmd.Flags().GetString("size")
	if sizeStr == "" {
		return errSizeRequired
	}
	sz, err := volumeadmin.ParseSize(sizeStr)
	if err != nil {
		return invalidSizeErr(err)
	}
	return volumeadmin.RunCreate(cmd.Context(), volumeadmin.CreateOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
		Size:        sz,
	})
}

func runVolumeInfo(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunInfo(cmd.Context(), volumeadmin.InfoOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
	})
}

func runVolumeStat(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunStat(cmd.Context(), volumeadmin.StatOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
	})
}

func runVolumeDelete(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunDelete(cmd.Context(), volumeadmin.DeleteOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
	})
}

func runVolumeResize(cmd *cobra.Command, args []string) error {
	sizeStr, _ := cmd.Flags().GetString("size")
	if sizeStr == "" {
		return errSizeRequired
	}
	sz, err := volumeadmin.ParseSize(sizeStr)
	if err != nil {
		return invalidSizeErr(err)
	}
	return volumeadmin.RunResize(cmd.Context(), volumeadmin.ResizeOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
		Size:        sz,
	})
}

func runVolumeRecalculate(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunRecalculate(cmd.Context(), volumeadmin.RecalculateOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
	})
}

func runVolumeWriteAt(cmd *cobra.Command, args []string) error {
	offset, _ := cmd.Flags().GetInt64("offset")
	content, _ := cmd.Flags().GetString("content")
	return volumeadmin.RunWriteAt(cmd.Context(), volumeadmin.WriteAtOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
		Offset:      offset,
		Content:     []byte(content),
	})
}

func runVolumeReadAt(cmd *cobra.Command, args []string) error {
	offset, _ := cmd.Flags().GetInt64("offset")
	length, _ := cmd.Flags().GetInt64("length")
	return volumeadmin.RunReadAt(cmd.Context(), volumeadmin.ReadAtOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
		Offset:      offset,
		Length:      length,
	})
}

func runVolumeScrub(cmd *cobra.Command, args []string) error {
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	detach, _ := cmd.Flags().GetBool("detach")
	return volumeadmin.RunScrub(cmd.Context(), volumeadmin.ScrubOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		Name:        args[0],
		DryRun:      dryRun,
		Detach:      detach,
	})
}

func runVolumeScrubStatus(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunScrubStatus(cmd.Context(), volumeadmin.ScrubStatusOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		SessionID:   args[0],
	})
}

func runVolumeScrubList(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunScrubList(cmd.Context(), volumeadmin.ScrubListOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
	})
}

func runVolumeScrubCancel(cmd *cobra.Command, args []string) error {
	return volumeadmin.RunScrubCancel(cmd.Context(), volumeadmin.ScrubCancelOptions{
		BaseOptions: baseOptionsFromCmd(cmd),
		SessionID:   args[0],
	})
}
