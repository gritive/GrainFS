package badgerrole

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
)

func OpenRole(reg Registry, role Role, ctx PathContext) (*badger.DB, Decision, error) {
	start := time.Now()
	spec, ok := reg.Get(role)
	if !ok {
		err := fmt.Errorf("badger role %q: unknown role", role)
		return nil, Decision{Role: role, Status: DecisionUnknownRole, Action: RecoveryActionBlockStart, Reason: err.Error(), Err: err}, err
	}
	path, err := reg.ResolvePath(role, ctx)
	if err != nil {
		return nil, Decision{Role: role, GroupID: ctx.GroupID, Status: DecisionOpenFailed, Action: actionFor(spec), Reason: err.Error(), Err: err}, err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		wrapped := fmt.Errorf("create %s dir: %w", role, err)
		return nil, Decision{Role: role, GroupID: ctx.GroupID, Path: path, Status: DecisionOpenFailed, Action: actionFor(spec), Reason: wrapped.Error(), Err: wrapped}, wrapped
	}
	if err := removeEmptyMemtables(path); err != nil {
		wrapped := fmt.Errorf("recover %s empty memtables: %w", role, err)
		return nil, Decision{Role: role, GroupID: ctx.GroupID, Path: path, Status: DecisionOpenFailed, Action: actionFor(spec), Reason: wrapped.Error(), Err: wrapped}, wrapped
	}

	opts := badgerutil.SmallOptions(path)
	if spec.OptionsKind == OptionsRaftLog {
		opts = badgerutil.RaftLogOptions(path, true)
	}
	db, err := badger.Open(opts)
	if err != nil {
		wrapped := fmt.Errorf("open %s badger at %s: %w", role, path, err)
		return nil, Decision{Role: role, GroupID: ctx.GroupID, Path: path, Status: DecisionOpenFailed, Action: actionFor(spec), Reason: wrapped.Error(), Err: wrapped, ProbeDuration: time.Since(start)}, wrapped
	}

	return db, Decision{Role: role, GroupID: ctx.GroupID, Path: path, Status: DecisionOK, Action: RecoveryActionNone, ProbeDuration: time.Since(start)}, nil
}

func removeEmptyMemtables(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".mem") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() != 0 {
			continue
		}
		if err := os.Remove(filepath.Join(path, entry.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func actionFor(spec RoleSpec) RecoveryAction {
	switch spec.Criticality {
	case CriticalityOptional:
		return RecoveryActionDisableFeature
	case CriticalityReadOnly:
		return RecoveryActionStartReadOnly
	default:
		return RecoveryActionBlockStart
	}
}
