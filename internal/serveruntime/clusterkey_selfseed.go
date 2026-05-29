package serveruntime

import (
	"errors"
	"fmt"
	"os"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
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
