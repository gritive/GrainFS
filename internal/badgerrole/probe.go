package badgerrole

import (
	"errors"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

var preflightSentinel = []byte("_sys:preflight")

type OpenFunc func(readOnly bool) (*badger.DB, error)

func PreflightSentinelKey() []byte {
	out := make([]byte, len(preflightSentinel))
	copy(out, preflightSentinel)
	return out
}

func ProbeReadOnly(role Role, groupID, path string, open OpenFunc) Decision {
	start := time.Now()
	db, err := open(true)
	if err != nil {
		return Decision{
			Role: role, GroupID: groupID, Path: path,
			Status: DecisionOpenFailed, Action: RecoveryActionBlockStart,
			Reason: fmt.Sprintf("read-only open failed: %v", err), Err: err,
			ProbeDuration: time.Since(start), OpenedReadOnly: true,
		}
	}
	defer db.Close()

	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		return nil
	})
	if err != nil {
		return Decision{
			Role: role, GroupID: groupID, Path: path,
			Status: DecisionReadOnlyProbeFailed, Action: RecoveryActionBlockStart,
			Reason: fmt.Sprintf("read-only probe failed: %v", err), Err: err,
			ProbeDuration: time.Since(start), OpenedReadOnly: true,
		}
	}

	return Decision{
		Role: role, GroupID: groupID, Path: path,
		Status: DecisionOK, Action: RecoveryActionNone,
		ProbeDuration: time.Since(start), OpenedReadOnly: true,
	}
}

func ProbeWritable(db *badger.DB, role Role, groupID, path string) Decision {
	start := time.Now()
	if db == nil {
		err := errors.New("nil badger DB")
		return Decision{
			Role: role, GroupID: groupID, Path: path,
			Status: DecisionWritableProbeFailed, Action: RecoveryActionBlockStart,
			Reason: err.Error(), Err: err, ProbeDuration: time.Since(start),
		}
	}

	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(preflightSentinel, []byte("ok"))
	}); err != nil {
		return writableProbeFailed(role, groupID, path, "write", err, start)
	}
	if err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(preflightSentinel)
		return err
	}); err != nil {
		return writableProbeFailed(role, groupID, path, "read", err, start)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Delete(preflightSentinel)
	}); err != nil {
		return writableProbeFailed(role, groupID, path, "delete", err, start)
	}

	return Decision{Role: role, GroupID: groupID, Path: path, Status: DecisionOK, Action: RecoveryActionNone, ProbeDuration: time.Since(start)}
}

func writableProbeFailed(role Role, groupID, path, op string, err error, start time.Time) Decision {
	return Decision{
		Role: role, GroupID: groupID, Path: path,
		Status: DecisionWritableProbeFailed, Action: RecoveryActionBlockStart,
		Reason: fmt.Sprintf("writable probe %s failed: %v", op, err), Err: err,
		ProbeDuration: time.Since(start),
	}
}
