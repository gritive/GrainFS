package serveruntime

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof" // pprof endpoints registered on DefaultServeMux when PprofPort > 0
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/iam"
	grainotel "github.com/gritive/GrainFS/internal/otel"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server"
)

// RunFromOptions is the cobra-free entry point for `grainfs serve`. It absorbs
// the assembly that used to live inline in cmd/grainfs/serve.go's runServe and
// delegates to the existing Run(ctx, Config). Assembly order matches the
// historical runServe body 1:1 (see spec §"RunFromOptions flow").
//
// The caller is responsible for building ctx (typically via signal.NotifyContext
// at the cmd layer); RunFromOptions itself does not install signal handlers.
func RunFromOptions(ctx context.Context, opts ServeOptions) error {
	// 1. Defaults for test seams.
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.Stderr == nil {
		opts.Stderr = os.Stderr
	}

	// 2. Badger value-threshold override (hidden test flag; global side effect,
	// matches cmd's pre-refactor ordering).
	if opts.BadgerValueThreshold > 0 {
		badgerutil.SetValueThresholdOverride(opts.BadgerValueThreshold)
	}

	// 3. IAM + s3auth wiring.
	iamStore := iam.NewStore()
	inner := s3auth.NewVerifier(nil)
	inner.SecretLookup = iam.NewSecretLookup(iamStore)
	verifier := s3auth.NewCachingVerifier(inner, 4096, 5*time.Minute)

	authOpts := []server.Option{
		server.WithVerifier(verifier),
		server.WithIAMStore(iamStore),
	}
	auditLogger := iam.NewAuditLogger(iam.NewLogAuditEmitter())
	authOpts = append(authOpts, server.WithIAMAudit(auditLogger))

	// 3b. Zero-CA invite-join (W9b): when an invite bundle is present and the
	// resume gate says FreshJoin/Resume, run Phase-1 over the dedicated QUIC join
	// transport to pull + stage the cluster bootstrap secrets (encryption.key,
	// KEK generations, cluster.id, transport PSK) BEFORE the earliest secret gate
	// (LoadOrCreateEncryptionKeyWithRaw below). It writes back opts.NodeID and
	// opts.ClusterKey so the normal boot resolves the identical node id and the
	// --cluster-key gate passes in-memory. The Phase-2 membership ACK runs
	// post-boot in bootWALAndForwarders.
	inviteJoin, err := maybeInviteJoin(ctx, &opts)
	if err != nil {
		return fmt.Errorf("zero-CA invite-join: %w", err)
	}

	// 4. Encryption key + IAMApplier.
	// Compute the canonical primary data dir the same way optionsToConfig does
	// (cfg.DataDir = cfg.DataDirs[0] when DataDirs is non-empty). The guard
	// marker and metaDir default must be written under this primary dir so they
	// agree with the rest of the boot sequence.
	primaryDataDir := opts.DataDir
	if len(opts.DataDirs) > 0 {
		primaryDataDir = opts.DataDirs[0]
	}
	// WithRaw variant returns the raw key bytes for the zero-CA invite-join
	// bootstrap-secret provider (sealed to a joiner); identical key-loading
	// semantics otherwise.
	shardEncryptor, rawEncryptionKey, err := LoadOrCreateEncryptionKeyWithRaw(
		opts.EncryptionKeyFile,
		opts.DataDir,
		AllowAutoGenerateEncryptionKey(opts.DataDir, opts.RaftAddr),
	)
	if err != nil {
		return fmt.Errorf("encryption setup: %w\n  recovery: pass --encryption-key-file=<path> to load an existing key", err)
	}
	metaDir := opts.MetaDir
	if metaDir == "" {
		metaDir = filepath.Join(primaryDataDir, "meta")
	}
	if err := EnsureBulkCipherFormat(primaryDataDir, BulkDataPresent(primaryDataDir, opts.DataDirs, metaDir)); err != nil {
		return fmt.Errorf("encryption format guard: %w", err)
	}
	iamApplier := iam.NewApplier(iamStore, shardEncryptor)

	// 5. pprof.
	if opts.PprofPort > 0 {
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)
		pprofAddr := fmt.Sprintf("127.0.0.1:%d", opts.PprofPort)
		go func() {
			log.Info().Str("addr", pprofAddr).Msg("pprof listening")
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Warn().Err(err).Msg("pprof server error")
			}
		}()
	}

	// 6. OTel init (defer shutdown only when endpoint is configured).
	otelShutdown, err := grainotel.Init(ctx, opts.OTelEndpoint, opts.OTelSampleRate)
	if err != nil {
		log.Warn().Err(err).Msg("otel: init failed, tracing disabled")
	} else if opts.OTelEndpoint != "" {
		log.Info().Str("endpoint", opts.OTelEndpoint).Float64("sample_rate", opts.OTelSampleRate).Msg("otel: tracing enabled")
		defer func() { _ = otelShutdown(context.Background()) }()
	}

	// 7. Preflight.
	addr := fmt.Sprintf(":%d", opts.Port)
	// opts.DataDir is the raw --data flag (may be a comma-separated multi-drive
	// list); preflight wants a single concrete path, so use the first drive
	// when DataDirs is populated. Skipping this lets MkdirAll inside
	// checkDataDir interpret the comma string literally and create a
	// nonsensical nested tree like "/path/d1,/path/d2,/...".
	preflightDataDir := opts.DataDir
	if len(opts.DataDirs) > 0 {
		preflightDataDir = opts.DataDirs[0]
	}
	if err := server.RunSystemPreflight(server.PreflightConfig{
		DataDir:  preflightDataDir,
		HTTPAddr: addr,
	}); err != nil {
		return err
	}

	// 8. Build Config from options.
	cfg := optionsToConfig(opts, addr, authOpts, shardEncryptor, iamStore, iamApplier)
	cfg.RawEncryptionKey = rawEncryptionKey
	cfg.InviteJoin = inviteJoin

	// 9. Delegate to existing Run.
	return Run(ctx, cfg)
}
