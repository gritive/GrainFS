package cluster

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

const (
	fsmValueFrameMagic   = "GFMV"
	fsmValueFrameVersion = byte(2)
	fsmValueFrameHeader  = 4 + 1 + 4
)

func (f *FSM) SetEncryptor(enc *encrypt.Encryptor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enc = enc
}

func (f *FSM) SetDEKKeeper(keeper *encrypt.DEKKeeper, clusterID []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dekKeeper = nil
	f.clusterID = [16]byte{}
	if keeper == nil || len(clusterID) != 16 {
		return
	}
	f.dekKeeper = keeper
	copy(f.clusterID[:], clusterID)
}

func (f *FSM) encryptor() *encrypt.Encryptor {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.enc
}

func (f *FSM) dataEncryptor() storage.DataEncryptor {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.dekKeeper != nil {
		return storage.NewDEKKeeperAdapter(f.dekKeeper, f.clusterID[:])
	}
	if f.enc != nil {
		var zero [16]byte
		return storage.NewEncryptorAdapter(f.enc, zero[:])
	}
	return nil
}

func fsmValueAAD(key []byte) []byte {
	aad := make([]byte, len("cluster-fsm-value:")+len(key))
	copy(aad, "cluster-fsm-value:")
	copy(aad[len("cluster-fsm-value:"):], key)
	return aad
}

func (f *FSM) sealValue(key []byte, plain []byte) ([]byte, error) {
	de := f.dataEncryptor()
	if de == nil {
		return plain, nil
	}
	ct, gen, err := de.Seal(encrypt.DomainFSMValue, []encrypt.AADField{encrypt.FieldBytes(key)}, plain)
	if err != nil {
		return nil, fmt.Errorf("seal cluster fsm value: %w", err)
	}
	return encodeFSMValueFrameV2(gen, ct), nil
}

func (f *FSM) openValue(key []byte, raw []byte) ([]byte, error) {
	if gen, ct, ok, err := decodeFSMValueFrameV2(raw); ok {
		if err != nil {
			return nil, err
		}
		de := f.dataEncryptor()
		if de == nil {
			return nil, fmt.Errorf("cluster fsm value is encrypted but data encryptor is not wired")
		}
		return de.Open(encrypt.DomainFSMValue, []encrypt.AADField{encrypt.FieldBytes(key)}, gen, ct)
	}
	enc := f.encryptor()
	if enc == nil {
		if encrypt.IsEncryptedValue(raw) {
			return nil, fmt.Errorf("cluster fsm value is encrypted but encryptor is not wired")
		}
		if encrypt.IsLegacyEncryptedValue(raw) {
			return nil, fmt.Errorf("cluster fsm value carries an unsupported/old encrypted-value format (pre-XAES); in-place upgrade unsupported")
		}
		return raw, nil
	}
	if !encrypt.IsEncryptedValue(raw) {
		if encrypt.IsLegacyEncryptedValue(raw) {
			return nil, fmt.Errorf("cluster fsm value carries an unsupported/old encrypted-value format (pre-XAES); in-place upgrade unsupported")
		}
		return raw, nil
	}
	return enc.OpenValueAAD(fsmValueAAD(key), raw)
}

func encodeFSMValueFrameV2(gen uint32, ct []byte) []byte {
	out := make([]byte, fsmValueFrameHeader+len(ct))
	copy(out[:4], fsmValueFrameMagic)
	out[4] = fsmValueFrameVersion
	binary.BigEndian.PutUint32(out[5:9], gen)
	copy(out[fsmValueFrameHeader:], ct)
	return out
}

func decodeFSMValueFrameV2(raw []byte) (gen uint32, ct []byte, ok bool, err error) {
	if len(raw) < 4 || string(raw[:4]) != fsmValueFrameMagic {
		return 0, nil, false, nil
	}
	if len(raw) < fsmValueFrameHeader {
		return 0, nil, true, fmt.Errorf("cluster fsm value frame truncated")
	}
	if raw[4] != fsmValueFrameVersion {
		return 0, nil, true, fmt.Errorf("unsupported cluster fsm value frame version %d", raw[4])
	}
	gen = binary.BigEndian.Uint32(raw[5:9])
	return gen, append([]byte(nil), raw[fsmValueFrameHeader:]...), true, nil
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
