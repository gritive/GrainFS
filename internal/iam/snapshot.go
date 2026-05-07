package iam

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// Snapshot binary format v1:
//
//	[u8 version=1]
//	[u8 authEnabled flag]
//	[u32 N_sas] [N_sas × (u32 len, FB SACreatePayload)]
//	[u32 N_keys] [N_keys × (u32 len, FB KeyCreatePayload)]   // SecretKeyEnc only, never plaintext
//	[u32 N_grants] [N_grants × (u32 len, FB GrantPutPayload)]
//	[u32 N_wildcards] [N_wildcards × (u32 len, FB GrantWildcardPutPayload)]
//	[u32 N_revoked] [N_revoked × (u32 len, raw access_key bytes)]
//
// All sizes are little-endian u32. Each FB blob is independently parseable.
const snapshotVersion uint8 = 1

// WriteSnapshot serializes the entire IAM store to w. SecretKey plaintexts
// are NEVER written; only SecretKeyEnc bytes are emitted.
func WriteSnapshot(w io.Writer, s *Store) error {
	st := s.snapshot()
	if _, err := w.Write([]byte{snapshotVersion}); err != nil {
		return err
	}
	authBit := byte(0)
	if st.authEnabled {
		authBit = 1
	}
	if _, err := w.Write([]byte{authBit}); err != nil {
		return err
	}

	if err := writeUint32(w, uint32(len(st.sas))); err != nil {
		return err
	}
	for _, sa := range st.sas {
		if err := writeBlob(w, buildSACreatePayload(*sa)); err != nil {
			return err
		}
	}

	if err := writeUint32(w, uint32(len(st.keysByAK))); err != nil {
		return err
	}
	for _, k := range st.keysByAK {
		if err := writeBlob(w, buildKeyCreatePayload(*k)); err != nil {
			return err
		}
	}

	totalGrants := 0
	for _, per := range st.grants {
		totalGrants += len(per)
	}
	if err := writeUint32(w, uint32(totalGrants)); err != nil {
		return err
	}
	for saID, per := range st.grants {
		for bucket, role := range per {
			if err := writeBlob(w, buildGrantPutPayload(Grant{SAID: saID, Bucket: bucket, Role: role})); err != nil {
				return err
			}
		}
	}

	if err := writeUint32(w, uint32(len(st.wildcards))); err != nil {
		return err
	}
	for saID, role := range st.wildcards {
		if err := writeBlob(w, buildGrantWildcardPutPayload(Grant{SAID: saID, Role: role})); err != nil {
			return err
		}
	}

	// Revoked AKs section — preserve revocation across restore.
	revokedAKs := make([]string, 0)
	for ak, k := range st.keysByAK {
		if k.Status == KeyStatusRevoked {
			revokedAKs = append(revokedAKs, ak)
		}
	}
	if err := writeUint32(w, uint32(len(revokedAKs))); err != nil {
		return err
	}
	for _, ak := range revokedAKs {
		if err := writeBlob(w, []byte(ak)); err != nil {
			return err
		}
	}
	return nil
}

// ReadSnapshot deserializes the snapshot bytes into dst. The Encryptor is
// required to decrypt SecretKeyEnc into in-memory plaintext SecretKey
// (matches FSM Apply path semantics).
func ReadSnapshot(r io.Reader, dst *Store, enc *encrypt.Encryptor) error {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return err
	}
	if hdr[0] != snapshotVersion {
		return fmt.Errorf("iam: unsupported snapshot version %d", hdr[0])
	}
	ap := NewApplier(dst, enc)

	nSA, err := readUint32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < nSA; i++ {
		blob, err := readBlob(r)
		if err != nil {
			return err
		}
		if err := ap.ApplySACreate(blob); err != nil {
			return err
		}
	}

	nK, err := readUint32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < nK; i++ {
		blob, err := readBlob(r)
		if err != nil {
			return err
		}
		if err := ap.ApplyKeyCreate(blob); err != nil {
			return err
		}
	}

	nG, err := readUint32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < nG; i++ {
		blob, err := readBlob(r)
		if err != nil {
			return err
		}
		if err := ap.ApplyGrantPut(blob); err != nil {
			return err
		}
	}

	nW, err := readUint32(r)
	if err != nil {
		return err
	}
	for i := uint32(0); i < nW; i++ {
		blob, err := readBlob(r)
		if err != nil {
			return err
		}
		if err := ap.ApplyGrantWildcardPut(blob); err != nil {
			return err
		}
	}

	nR, err := readUint32(r)
	if err != nil {
		// Older snapshots without the revoked section: io.EOF means zero bytes
		// read (end of stream), which is acceptable. Any other error is real.
		if errors.Is(err, io.EOF) {
			nR = 0
		} else {
			return err
		}
	}
	for i := uint32(0); i < nR; i++ {
		blob, err := readBlob(r)
		if err != nil {
			return err
		}
		dst.applyKeyRevoke(string(blob))
	}

	if hdr[1] == 1 {
		dst.applyAuthEnable()
	}
	return nil
}

func buildSACreatePayload(sa ServiceAccount) []byte {
	b := flatbuffers.NewBuilder(64)
	id := b.CreateString(sa.ID)
	name := b.CreateString(sa.Name)
	desc := b.CreateString(sa.Description)
	cb := b.CreateString(sa.CreatedBy)
	iampb.SACreatePayloadStart(b)
	iampb.SACreatePayloadAddSaId(b, id)
	iampb.SACreatePayloadAddName(b, name)
	iampb.SACreatePayloadAddDescription(b, desc)
	iampb.SACreatePayloadAddCreatedAtUnixNs(b, sa.CreatedAt.UnixNano())
	iampb.SACreatePayloadAddCreatedBy(b, cb)
	b.Finish(iampb.SACreatePayloadEnd(b))
	return b.FinishedBytes()
}

func buildKeyCreatePayload(k AccessKey) []byte {
	b := flatbuffers.NewBuilder(128)
	ak := b.CreateString(k.AccessKey)
	sa := b.CreateString(k.SAID)
	encVec := b.CreateByteVector(k.SecretKeyEnc)
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, ak)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encVec)
	iampb.KeyCreatePayloadAddSaId(b, sa)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, k.CreatedAt.UnixNano())
	if k.ExpiresAt != nil {
		iampb.KeyCreatePayloadAddExpiresAtUnixNs(b, k.ExpiresAt.UnixNano())
	}
	b.Finish(iampb.KeyCreatePayloadEnd(b))
	return b.FinishedBytes()
}

func buildGrantPutPayload(g Grant) []byte {
	b := flatbuffers.NewBuilder(64)
	sa := b.CreateString(g.SAID)
	bk := b.CreateString(g.Bucket)
	cb := b.CreateString(g.CreatedBy)
	iampb.GrantPutPayloadStart(b)
	iampb.GrantPutPayloadAddSaId(b, sa)
	iampb.GrantPutPayloadAddBucket(b, bk)
	iampb.GrantPutPayloadAddRole(b, iampb.Role(g.Role))
	iampb.GrantPutPayloadAddCreatedAtUnixNs(b, g.CreatedAt.UnixNano())
	iampb.GrantPutPayloadAddCreatedBy(b, cb)
	b.Finish(iampb.GrantPutPayloadEnd(b))
	return b.FinishedBytes()
}

func buildGrantWildcardPutPayload(g Grant) []byte {
	b := flatbuffers.NewBuilder(64)
	sa := b.CreateString(g.SAID)
	cb := b.CreateString(g.CreatedBy)
	iampb.GrantWildcardPutPayloadStart(b)
	iampb.GrantWildcardPutPayloadAddSaId(b, sa)
	iampb.GrantWildcardPutPayloadAddRole(b, iampb.Role(g.Role))
	iampb.GrantWildcardPutPayloadAddCreatedAtUnixNs(b, g.CreatedAt.UnixNano())
	iampb.GrantWildcardPutPayloadAddCreatedBy(b, cb)
	b.Finish(iampb.GrantWildcardPutPayloadEnd(b))
	return b.FinishedBytes()
}

func buildSADeletePayload(saID string) []byte {
	b := flatbuffers.NewBuilder(32)
	idOff := b.CreateString(saID)
	iampb.SADeletePayloadStart(b)
	iampb.SADeletePayloadAddSaId(b, idOff)
	b.Finish(iampb.SADeletePayloadEnd(b))
	return b.FinishedBytes()
}

func buildKeyRevokePayload(accessKey string) []byte {
	b := flatbuffers.NewBuilder(32)
	akOff := b.CreateString(accessKey)
	iampb.KeyRevokePayloadStart(b)
	iampb.KeyRevokePayloadAddAccessKey(b, akOff)
	b.Finish(iampb.KeyRevokePayloadEnd(b))
	return b.FinishedBytes()
}

func buildGrantWildcardDeletePayload(saID string) []byte {
	b := flatbuffers.NewBuilder(32)
	saOff := b.CreateString(saID)
	iampb.GrantWildcardDeletePayloadStart(b)
	iampb.GrantWildcardDeletePayloadAddSaId(b, saOff)
	b.Finish(iampb.GrantWildcardDeletePayloadEnd(b))
	return b.FinishedBytes()
}

func buildGrantDeletePayload(saID, bucket string) []byte {
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	bkOff := b.CreateString(bucket)
	iampb.GrantDeletePayloadStart(b)
	iampb.GrantDeletePayloadAddSaId(b, saOff)
	iampb.GrantDeletePayloadAddBucket(b, bkOff)
	b.Finish(iampb.GrantDeletePayloadEnd(b))
	return b.FinishedBytes()
}

func buildAuthEnablePayload() []byte {
	b := flatbuffers.NewBuilder(8)
	iampb.AuthEnablePayloadStart(b)
	b.Finish(iampb.AuthEnablePayloadEnd(b))
	return b.FinishedBytes()
}

func writeUint32(w io.Writer, v uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func writeBlob(w io.Writer, blob []byte) error {
	if err := writeUint32(w, uint32(len(blob))); err != nil {
		return err
	}
	_, err := w.Write(blob)
	return err
}

func readBlob(r io.Reader) ([]byte, error) {
	n, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
