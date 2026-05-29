package serveruntime

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/rs/zerolog/log"
)

// Fail-closed probes for the genesis self-seed predicate.
//
// The shared helpers dirHasContent / fileExists / keysDirHasKEK fail OPEN
// (return false/absent on any error) because their other consumers
// (wireDEKKeeper, isGenesisBoot, gateInviteJoin) want that. Self-seed must NOT
// reuse them: a populated data dir on a transiently failing or mis-permissioned
// mount, restarted with no --cluster-key, would be misread as "fresh" and
// self-seed a new identity OVER the existing one. These local probes distinguish
// os.ErrNotExist (truly absent) from any other error (surfaced → caller blocks).

// dirEmptyStrict reports whether dir is empty, failing closed: a missing dir is
// empty (nil err); any other read error is returned.
func dirEmptyStrict(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("self-seed probe: read dir %s: %w", dir, err)
	}
	return len(entries) == 0, nil
}

// fileAbsentStrict reports whether path is absent, failing closed: os.ErrNotExist
// → absent (nil err); any other stat error is returned.
func fileAbsentStrict(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("self-seed probe: stat %s: %w", path, err)
	}
	return false, nil
}

// kekDirEmptyStrict reports whether the effective KEK dir holds no keys/<N>.key.
// encrypt.KeysDirIsEmpty already treats a missing dir as empty and returns an
// error for a non-listable path (e.g. the dir is actually a file → ENOTDIR);
// surface that error instead of swallowing it to false.
func kekDirEmptyStrict(kekDir string) (bool, error) {
	empty, err := encrypt.KeysDirIsEmpty(kekDir)
	if err != nil {
		return false, fmt.Errorf("self-seed probe: kek dir %s: %w", kekDir, err)
	}
	return empty, nil
}

// currentKeyState reports whether keys.d/current.key holds a usable PSK.
// have=true → present (restart). have=false,nil → truly absent (os.ErrNotExist).
// err!=nil → unreadable; the caller MUST block and never WriteCurrent over it.
func currentKeyState(ks *transport.Keystore) (have bool, err error) {
	if _, rerr := ks.ReadCurrent(); rerr == nil {
		return true, nil
	} else if errors.Is(rerr, os.ErrNotExist) {
		return false, nil
	} else {
		return false, fmt.Errorf("self-seed probe: read current.key: %w", rerr)
	}
}

// generateRandomClusterKey returns 32 random bytes hex-encoded (64 chars).
// Unlike the legacy GenerateEphemeralClusterKey ("never leaves this process"),
// a self-seeded key is persisted and pulled by invite-join peers — it
// deliberately DOES leave this process.
func generateRandomClusterKey() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("self-seed: random key: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// selfSeedDecision evaluates the genesis self-seed predicate (spec conditions
// 1,3,4,5,6,7 — condition 2 is checked by the caller via currentKeyState before
// reaching here). Returns (true,nil) only when EVERY condition holds; (false,nil)
// for a non-genesis / already-provisioned boot; (false,err) when any probe hits a
// non-ErrNotExist error (fail closed — caller surfaces, never seeds).
func selfSeedDecision(state *bootState) (bool, error) {
	if state.cfg.ClusterKey != "" { // cond 1
		return false, nil
	}
	if state.joinMode || state.inviteJoinMode || len(state.peers) > 0 { // cond 6 (genesis)
		return false, nil
	}
	dataDir := state.cfg.DataDir

	// cond 3: invite-join artifacts (covers both Phase-1 crash windows).
	for _, name := range []string{"node.key.unsealed", "node.key.enc", ".invite-join-pending"} {
		absent, err := fileAbsentStrict(filepath.Join(dataDir, name))
		if err != nil {
			return false, err
		}
		if !absent {
			return false, nil
		}
	}

	// cond 7: no cluster.id — never adopt a foreign/staged cluster identity.
	if absent, err := fileAbsentStrict(filepath.Join(dataDir, nodeconfig.ClusterIDFile)); err != nil {
		return false, err
	} else if !absent {
		return false, nil
	}

	// cond 4: empty effective KEK dir (honors GRAINFS_KEK_DIR).
	if empty, err := kekDirEmptyStrict(nodeconfig.New(dataDir).KEKDir()); err != nil {
		return false, err
	} else if !empty {
		return false, nil
	}

	// cond 5: !priorState — meta and raft dirs empty.
	for _, d := range []string{state.metaDir, state.raftDir} {
		if empty, err := dirEmptyStrict(d); err != nil {
			return false, err
		} else if !empty {
			return false, nil
		}
	}
	return true, nil
}

// resolveOrSeedClusterKey runs just before the --cluster-key gate in
// bootValidateConfig. It loads an existing disk key (restart), self-seeds a fresh
// genesis, or leaves the key empty so the existing ErrEmptyClusterKey gate fires.
//
// It reads keys.d/current.key DIRECTLY with a 3-way switch and does NOT reuse
// ResolveClusterKey: that helper collapses all read errors (hasDisk := diskErr ==
// nil), which would (a) overwrite an unreadable slot and (b) confuse a post-drop
// local placeholder with a real PSK. Self-seed must fail closed and stay distinct.
func resolveOrSeedClusterKey(state *bootState) error {
	if state.cfg.ClusterKey != "" {
		return nil // flag path; ResolveClusterKey mirrors it to disk later
	}
	ks := transport.NewKeystore(state.cfg.DataDir)
	have, err := currentKeyState(ks) // cond 2, fail-closed
	if err != nil {
		return err
	}
	if have {
		return nil // restart: disk key wins; bootQUICTransport's ResolveClusterKey loads it
	}
	seed, err := selfSeedDecision(state)
	if err != nil {
		return err
	}
	if !seed {
		return nil // leave empty → existing ErrEmptyClusterKey gate fires
	}
	key, err := generateRandomClusterKey()
	if err != nil {
		return err
	}
	if err := ks.WriteCurrent(key); err != nil {
		return fmt.Errorf("self-seed: persist current.key: %w", err)
	}
	state.cfg.ClusterKey = key
	log.Warn().
		Str("node_id", state.nodeID).
		Str("data_dir", state.cfg.DataDir).
		Bool("self_seeded", true).
		Msg("genesis self-seed: no --cluster-key/invite-bundle/peers and data dir empty — " +
			"bootstrapping a NEW single-node cluster with a self-generated key. " +
			"If you meant to JOIN an existing cluster, stop and set GRAINFS_INVITE_BUNDLE.")
	return nil
}
