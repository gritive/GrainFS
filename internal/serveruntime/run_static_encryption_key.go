package serveruntime

import (
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func loadStaticEncryptionKeyForRun(opts ServeOptions, primaryDataDir string, inviteJoin *inviteJoinState) (*encrypt.Encryptor, []byte, error) {
	encryptionKeyPath := opts.EncryptionKeyFile
	if encryptionKeyPath == "" {
		encryptionKeyPath = filepath.Join(primaryDataDir, "encryption.key")
	}
	if opts.EncryptionKeyFile == "" && !fileExists(encryptionKeyPath) && (inviteJoin != nil || staticKeylessClusterBootReady(primaryDataDir, opts.ClusterKey)) {
		log.Info().
			Str("component", "server").
			Msg("static encryption key absent for zero-CA member; continuing with KEK-backed boot")
		return nil, nil, nil
	}
	return LoadOrCreateEncryptionKeyWithRaw(
		opts.EncryptionKeyFile,
		primaryDataDir,
		AllowAutoGenerateEncryptionKey(primaryDataDir, opts.RaftAddr),
	)
}

func staticKeylessClusterBootReady(dataDir, clusterKey string) bool {
	p := inviteJoinPathsFor(dataDir)
	return fileExists(p.clusterID) &&
		keysDirHasKEK(p.keysDir) &&
		fileExists(p.nodeKeyEnc) &&
		fileExists(filepath.Join(dataDir, "keys.d", nodeKeyGenFile)) &&
		(fileExists(p.currentKey) || clusterKey != "")
}
