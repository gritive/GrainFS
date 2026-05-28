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

const (
	badgerDomainBucket    = "badger:local:bucket"
	badgerDomainObject    = "badger:local:object"
	badgerDomainMultipart = "badger:local:multipart"
	badgerDomainPolicy    = "badger:local:policy"
)

func setBadgerValue(txn *badger.Txn, enc *encrypt.Encryptor, domain string, key, plain []byte) error {
	if enc == nil {
		return txn.Set(key, plain)
	}
	sealed, err := enc.SealValueAADTo(nil, badgerValueAAD(domain, key), plain)
	if err != nil {
		return fmt.Errorf("encrypt badger value %s: %w", domain, err)
	}
	return txn.Set(key, sealed)
}

func openBadgerValue(enc *encrypt.Encryptor, domain string, key, val []byte) ([]byte, error) {
	if enc == nil {
		if encrypt.IsLegacyEncryptedValue(val) {
			return nil, fmt.Errorf("value carries an unsupported/old encrypted-value format (pre-XAES); in-place upgrade unsupported")
		}
		return append([]byte(nil), val...), nil
	}
	if !encrypt.IsEncryptedValue(val) {
		if encrypt.IsLegacyEncryptedValue(val) {
			return nil, fmt.Errorf("value carries an unsupported/old encrypted-value format (pre-XAES); in-place upgrade unsupported")
		}
		return append([]byte(nil), val...), nil
	}
	return enc.OpenValueAAD(badgerValueAAD(domain, key), val)
}

func getBadgerValue(txn *badger.Txn, enc *encrypt.Encryptor, domain string, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = item.Value(func(val []byte) error {
		var openErr error
		out, openErr = openBadgerValue(enc, domain, key, val)
		return openErr
	})
	return out, err
}

func badgerValueAAD(domain string, key []byte) []byte {
	aad := make([]byte, 0, len(domain)+1+len(key))
	aad = append(aad, domain...)
	aad = append(aad, 0)
	aad = append(aad, key...)
	return aad
}
