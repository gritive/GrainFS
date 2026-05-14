package cluster

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func (f *FSM) SetEncryptor(enc *encrypt.Encryptor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enc = enc
}

func (f *FSM) encryptor() *encrypt.Encryptor {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.enc
}

func fsmValueAAD(key []byte) []byte {
	aad := make([]byte, len("cluster-fsm-value:")+len(key))
	copy(aad, "cluster-fsm-value:")
	copy(aad[len("cluster-fsm-value:"):], key)
	return aad
}

func (f *FSM) sealValue(key []byte, plain []byte) ([]byte, error) {
	enc := f.encryptor()
	if enc == nil {
		return plain, nil
	}
	return enc.SealValueAADTo(nil, fsmValueAAD(key), plain)
}

func (f *FSM) openValue(key []byte, raw []byte) ([]byte, error) {
	enc := f.encryptor()
	if enc == nil {
		if encrypt.IsEncryptedValue(raw) {
			return nil, fmt.Errorf("cluster fsm value is encrypted but encryptor is not wired")
		}
		return raw, nil
	}
	if !encrypt.IsEncryptedValue(raw) {
		return raw, nil
	}
	return enc.OpenValueAAD(fsmValueAAD(key), raw)
}

func (f *FSM) setValue(txn *badger.Txn, key []byte, plain []byte) error {
	val, err := f.sealValue(key, plain)
	if err != nil {
		return err
	}
	return txn.Set(key, val)
}

func (f *FSM) itemValueCopy(item *badger.Item) ([]byte, error) {
	key := item.KeyCopy(nil)
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return f.openValue(key, raw)
}

func (b *DistributedBackend) itemValueCopy(item *badger.Item) ([]byte, error) {
	if b.fsm == nil {
		return item.ValueCopy(nil)
	}
	return b.fsm.itemValueCopy(item)
}
