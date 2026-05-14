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
	sealed, err := enc.SealValue(domain, plain)
	if err != nil {
		return fmt.Errorf("encrypt badger value %s: %w", domain, err)
	}
	return txn.Set(key, sealed)
}

func getBadgerValue(txn *badger.Txn, enc *encrypt.Encryptor, domain string, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = item.Value(func(val []byte) error {
		if enc == nil {
			out = append([]byte(nil), val...)
			return nil
		}
		plain, openErr := enc.OpenValue(domain, val)
		if openErr != nil {
			return openErr
		}
		out = plain
		return nil
	})
	return out, err
}
