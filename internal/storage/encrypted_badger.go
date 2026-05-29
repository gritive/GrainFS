// TEST-FIXTURE ONLY — not part of the production storage path.
//
// LocalBackend has zero production callers (audited 2026-05-28). It is a unit-test
// fixture used by 60+ external *_test.go files and many in-package tests. The
// production storage path is ClusterCoordinator → DistributedBackend (see
// boot_phases_storage_runtime.go). Do not add a non-test caller for any symbol
// declared in this file or in its companions (local.go, multipart.go, append.go,
// encrypted_badger.go) without revisiting ADR-0015.
//
// See: docs/adr/0015-localbackend-test-fixture-only.md

package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// LocalBackend badger meta is plaintext: the static value-sealing meta encryptor
// was retired in R3 (the data-at-rest seam is b.segEnc, which protects object and
// segment files, not badger meta). These helpers keep the pre-XAES loud-fail
// boundary — a value carrying the old encrypted-value envelope must error rather
// than be served as plaintext — and otherwise pass meta through verbatim.

func setBadgerValue(txn *badger.Txn, key, plain []byte) error {
	return txn.Set(key, plain)
}

func openBadgerValue(val []byte) ([]byte, error) {
	if encrypt.IsLegacyEncryptedValue(val) {
		return nil, fmt.Errorf("value carries an unsupported/old encrypted-value format (pre-XAES); in-place upgrade unsupported")
	}
	return append([]byte(nil), val...), nil
}

func getBadgerValue(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = item.Value(func(val []byte) error {
		var openErr error
		out, openErr = openBadgerValue(val)
		return openErr
	})
	return out, err
}
