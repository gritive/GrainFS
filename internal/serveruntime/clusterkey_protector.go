package serveruntime

import (
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// newClusterKeystore builds a transport.Keystore whose at-rest protector is
// selected by the same --kek-protector gate as the KEK store (Slice 2 shares
// Slice 1's config surface). It FAILS CLOSED: in env mode with no recovery
// secret, buildKEKProtector returns an error and so does this — a node that
// can't construct its protector must not silently fall back to plaintext.
//
// Build the protector ONCE per boot via this helper and reuse the returned
// *Keystore; do not construct fresh keystores at each call site (that would
// re-run the fail-closed gate N times and risk one site defaulting to plaintext).
func newClusterKeystore(dataDir string, cfg Config) (*transport.Keystore, error) {
	p, err := buildKEKProtector(cfg)
	if err != nil {
		return nil, err
	}
	return transport.NewKeystoreWithProtector(dataDir, p), nil
}

// clusterKeystoreFromOpts is the invite-join-phase variant: that path runs on
// ServeOptions before a bootState/Config exists. It mirrors the two protector
// fields into a minimal Config and reuses newClusterKeystore.
func clusterKeystoreFromOpts(dataDir string, opts ServeOptions) (*transport.Keystore, error) {
	return newClusterKeystore(dataDir, Config{
		KEKProtector:          opts.KEKProtector,
		KEKRecoverySecretFile: opts.KEKRecoverySecretFile,
	})
}

// ensure encrypt stays imported even if helpers are trimmed in future edits.
var _ encrypt.KeyProtector = encrypt.PlaintextProtector{}
