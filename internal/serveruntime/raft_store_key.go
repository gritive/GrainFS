package serveruntime

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
)

const (
	raftStoreKeySize        = 32
	raftStoreKeyEnvelopeV1  = 1
	raftStoreKeySidecarFile = "raft-store.key.enc"
)

type raftStoreKeyEnvelope struct {
	Version    uint32 `json:"version"`
	KEKVer     uint32 `json:"kek_ver"`
	Ciphertext []byte `json:"ciphertext"`
}

type raftStoreKeyMeta struct {
	KEKVersion uint32
	Path       string
}

func loadOrCreateRaftStoreKey(dataDir string, store *encrypt.KEKStore, clusterID []byte, nodeID string, existingRaftStore bool) ([]byte, raftStoreKeyMeta, error) {
	if err := validateRaftStoreKeyContext(clusterID, nodeID); err != nil {
		return nil, raftStoreKeyMeta{}, err
	}
	path := raftStoreKeyPath(dataDir)
	env, err := readRaftStoreKeyEnvelope(path)
	switch {
	case err == nil:
		key, err := openRaftStoreKeyEnvelope(env, store, clusterID, nodeID)
		if err != nil {
			return nil, raftStoreKeyMeta{}, fmt.Errorf("open %s: %w", path, err)
		}
		return key, raftStoreKeyMeta{KEKVersion: env.KEKVer, Path: path}, nil
	case errors.Is(err, os.ErrNotExist):
		if existingRaftStore {
			return nil, raftStoreKeyMeta{}, fmt.Errorf("%s is missing; encrypted raft log in-place migration is unsupported", path)
		}
	default:
		return nil, raftStoreKeyMeta{}, err
	}

	key := make([]byte, raftStoreKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, raftStoreKeyMeta{}, fmt.Errorf("generate raft store key: %w", err)
	}
	meta, err := writeRaftStoreKeyEnvelope(path, store, clusterID, nodeID, key)
	if err != nil {
		for i := range key {
			key[i] = 0
		}
		return nil, raftStoreKeyMeta{}, err
	}
	return key, meta, nil
}

func bootRaftStoreKey(state *bootState) error {
	if err := loadKEKStoreAndClusterID(state); err != nil {
		return err
	}
	exists, err := raftStoreExists(state.raftDir)
	if err != nil {
		return err
	}
	metaExists, err := raftStoreExists(filepath.Join(state.cfg.DataDir, "meta_raft"))
	if err != nil {
		return err
	}
	exists = exists || metaExists
	key, meta, err := loadOrCreateRaftStoreKey(state.cfg.DataDir, state.kekStore, state.clusterID, state.nodeID, exists)
	if err != nil {
		return err
	}
	state.raftStoreKey = key
	state.raftStoreKeyKEKVer.Store(meta.KEKVersion)
	state.AddCleanup(func() { zeroizeBytes(key) })
	return nil
}

type raftStoreKeyPostCommitDispatcher struct {
	state *bootState
}

func (d raftStoreKeyPostCommitDispatcher) Handle(cmdType clusterpb.MetaCmdType, _ []byte) {
	if cmdType != clusterpb.MetaCmdTypeKEKRotate || d.state == nil {
		return
	}
	go func() {
		version, err := rewrapRaftStoreKey(d.state.cfg.DataDir, d.state.kekStore, d.state.clusterID, d.state.nodeID)
		if err != nil {
			log.Warn().Err(err).Msg("raft_store_key: rewrap after KEK rotation failed")
			return
		}
		d.state.raftStoreKeyKEKVer.Store(version)
	}()
}

func wireRaftStoreKeyPostCommit(state *bootState, fsm *cluster.MetaFSM) {
	fsm.RegisterPostCommit(raftStoreKeyPostCommitDispatcher{state: state}.Handle)
}

func checkRaftStoreKeyPruneRef(state *bootState, version uint32) error {
	if state == nil {
		return nil
	}
	if state.raftStoreKeyKEKVer.Load() == version {
		return fmt.Errorf("cannot prune KEK v%d: raft-store key is still sealed under this version", version)
	}
	return nil
}

func rewrapRaftStoreKey(dataDir string, store *encrypt.KEKStore, clusterID []byte, nodeID string) (uint32, error) {
	if err := validateRaftStoreKeyContext(clusterID, nodeID); err != nil {
		return 0, err
	}
	path := raftStoreKeyPath(dataDir)
	env, err := readRaftStoreKeyEnvelope(path)
	if err != nil {
		return 0, err
	}
	key, err := openRaftStoreKeyEnvelope(env, store, clusterID, nodeID)
	if err != nil {
		return 0, fmt.Errorf("open %s before rewrap: %w", path, err)
	}
	defer zeroizeBytes(key)
	meta, err := writeRaftStoreKeyEnvelope(path, store, clusterID, nodeID, key)
	if err != nil {
		return 0, err
	}
	return meta.KEKVersion, nil
}

func raftStoreKeyPath(dataDir string) string {
	return filepath.Join(dataDir, "keys.d", raftStoreKeySidecarFile)
}

func raftStoreExists(raftDir string) (bool, error) {
	storeDir := filepath.Join(raftDir, "raft-v2")
	entries, err := os.ReadDir(storeDir)
	if err == nil {
		return len(entries) > 0, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("read raft store dir %s: %w", storeDir, err)
}

func validateRaftStoreKeyContext(clusterID []byte, nodeID string) error {
	if len(clusterID) != 16 {
		return fmt.Errorf("raft store key cluster_id len = %d, want 16", len(clusterID))
	}
	if nodeID == "" {
		return fmt.Errorf("raft store key node_id is empty")
	}
	return nil
}

func raftStoreKeyAAD(clusterID []byte, nodeID string) []byte {
	return encrypt.BuildAAD(encrypt.DomainRaftStoreKey, clusterID, encrypt.FieldString(nodeID))
}

func readRaftStoreKeyEnvelope(path string) (raftStoreKeyEnvelope, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return raftStoreKeyEnvelope{}, err
	}
	defer f.Close()
	var env raftStoreKeyEnvelope
	if err := json.NewDecoder(f).Decode(&env); err != nil {
		return raftStoreKeyEnvelope{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if env.Version != raftStoreKeyEnvelopeV1 {
		return raftStoreKeyEnvelope{}, fmt.Errorf("unsupported raft store key envelope version %d", env.Version)
	}
	if len(env.Ciphertext) == 0 {
		return raftStoreKeyEnvelope{}, fmt.Errorf("raft store key envelope has empty ciphertext")
	}
	return env, nil
}

func openRaftStoreKeyEnvelope(env raftStoreKeyEnvelope, store *encrypt.KEKStore, clusterID []byte, nodeID string) ([]byte, error) {
	kek, err := store.Get(env.KEKVer)
	if err != nil {
		return nil, fmt.Errorf("get KEK version %d: %w", env.KEKVer, err)
	}
	plain, err := encrypt.AESGCMOpenWithAAD(kek, env.Ciphertext, raftStoreKeyAAD(clusterID, nodeID))
	if err != nil {
		return nil, err
	}
	if len(plain) != raftStoreKeySize {
		zeroizeBytes(plain)
		return nil, fmt.Errorf("raft store key len = %d, want %d", len(plain), raftStoreKeySize)
	}
	return plain, nil
}

func writeRaftStoreKeyEnvelope(path string, store *encrypt.KEKStore, clusterID []byte, nodeID string, key []byte) (raftStoreKeyMeta, error) {
	if len(key) != raftStoreKeySize {
		return raftStoreKeyMeta{}, fmt.Errorf("raft store key len = %d, want %d", len(key), raftStoreKeySize)
	}
	kekVer := store.ActiveVersion()
	kek, err := store.Get(kekVer)
	if err != nil {
		return raftStoreKeyMeta{}, fmt.Errorf("get active KEK version %d: %w", kekVer, err)
	}
	ct, err := encrypt.AESGCMSealWithAAD(kek, key, raftStoreKeyAAD(clusterID, nodeID))
	if err != nil {
		return raftStoreKeyMeta{}, fmt.Errorf("seal raft store key: %w", err)
	}
	env := raftStoreKeyEnvelope{
		Version:    raftStoreKeyEnvelopeV1,
		KEKVer:     kekVer,
		Ciphertext: ct,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return raftStoreKeyMeta{}, fmt.Errorf("marshal raft store key envelope: %w", err)
	}
	if err := writePrivateFileAtomic(path, raw); err != nil {
		return raftStoreKeyMeta{}, err
	}
	return raftStoreKeyMeta{KEKVersion: kekVer, Path: path}, nil
}

func writePrivateFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("open tmp %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync tmp %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

func zeroizeBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
