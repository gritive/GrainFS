package serveruntime

import (
	"fmt"
	"os"
	"strings"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// kekRecoverySecretEnv is the environment variable holding the env-protector
// recovery passphrase. It takes precedence over the file path in Config.
const kekRecoverySecretEnv = "GRAINFS_KEK_RECOVERY_SECRET"

// buildKEKProtector constructs the at-rest KEK protector selected by config.
// Default (empty or "plaintext") is the identity protector, so <V>.key files
// stay byte-identical. "env" binds the KEK to machine factors with a recovery
// passphrase slot.
//
// env mode REQUIRES the recovery secret to be resident for the lifetime of the
// service: buildKEKProtector fails closed if it is absent. This is enforced once
// at boot (see preflightKEKProtector) so that KEK rotation — which persists via
// KEKStore.AddAndPersist -> protector.Protect on the replicated FSM Apply path —
// can never fatal-halt a node mid-rotation because a secret went missing. The
// secret is read eagerly here, but on the happy unwrap path EnvProtector still
// avoids the expensive Argon2id derivation (env slot opens with HKDF only).
func buildKEKProtector(cfg Config) (encrypt.KeyProtector, error) {
	switch cfg.KEKProtector {
	case "", "plaintext":
		return encrypt.PlaintextProtector{}, nil
	case "env":
		secret, err := resolveRecoverySecret(cfg)
		if err != nil {
			return nil, fmt.Errorf("kek protector env: %w", err)
		}
		if len(secret) == 0 {
			return nil, fmt.Errorf("kek protector env requires a recovery secret: set %s or --kek-recovery-secret-file (env mode keeps the secret resident so KEK rotation cannot fatal-halt a node)", kekRecoverySecretEnv)
		}
		return encrypt.NewEnvProtector(func() ([]byte, error) {
			return resolveRecoverySecret(cfg)
		}), nil
	default:
		return nil, fmt.Errorf("unknown kek protector %q (want \"plaintext\" or \"env\")", cfg.KEKProtector)
	}
}

// resolveRecoverySecret reads the env-protector recovery passphrase from the
// environment (preferred) or the configured file path. Whitespace-only / unset
// resolves to an empty secret, which the protector treats as "absent" so create
// and recovery surface a precise error.
func resolveRecoverySecret(cfg Config) ([]byte, error) {
	if v := strings.TrimSpace(os.Getenv(kekRecoverySecretEnv)); v != "" {
		return []byte(v), nil
	}
	if cfg.KEKRecoverySecretFile != "" {
		b, err := os.ReadFile(cfg.KEKRecoverySecretFile)
		if err != nil {
			return nil, fmt.Errorf("read kek recovery secret file %q: %w", cfg.KEKRecoverySecretFile, err)
		}
		if s := strings.TrimSpace(string(b)); s != "" {
			return []byte(s), nil
		}
	}
	return nil, nil
}
