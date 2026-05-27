package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/gritive/GrainFS/internal/protocred"
)

const (
	protocolCredentialRequestCreate    = "create"
	protocolCredentialRequestRotate    = "rotate"
	protocolCredentialRequestRevoke    = "revoke"
	protocolCredentialRequestMarkStale = "mark_stale"
)

func (f *MetaFSM) applyProtocolCredentialCreate(payload []byte) error {
	cmd, err := decodeProtocolCredentialCreateCmd(payload)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protocolCredentialStore == nil {
		return nil
	}
	payloadHash := protocolCredentialCreateDigest(cmd)
	replay, err := f.checkProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestCreate, cmd.Credential.ID, payloadHash)
	if err != nil || replay {
		return err
	}
	_, err = f.protocolCredentialStore.ApplyCreate(cmd.Credential)
	if err != nil {
		return err
	}
	f.recordProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestCreate, cmd.Credential.ID, payloadHash)
	return nil
}

func (f *MetaFSM) applyProtocolCredentialRotate(payload []byte) error {
	cmd, err := decodeProtocolCredentialRotateCmd(payload)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protocolCredentialStore == nil {
		return nil
	}
	payloadHash := protocolCredentialRotateDigest(cmd)
	replay, err := f.checkProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestRotate, cmd.ID, payloadHash)
	if err != nil || replay {
		return err
	}
	_, err = f.protocolCredentialStore.ApplyRotate(cmd.ID, cmd.SecretHash, cmd.SecretHint)
	if err != nil {
		return err
	}
	f.recordProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestRotate, cmd.ID, payloadHash)
	return nil
}

func (f *MetaFSM) applyProtocolCredentialRevoke(payload []byte) error {
	cmd, err := decodeProtocolCredentialRevokeCmd(payload)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protocolCredentialStore == nil {
		return nil
	}
	payloadHash := protocolCredentialRevokeDigest(cmd)
	replay, err := f.checkProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestRevoke, cmd.ID, payloadHash)
	if err != nil || replay {
		return err
	}
	_, err = f.protocolCredentialStore.ApplyRevoke(cmd.ID, cmd.RevokedAt)
	if err != nil {
		return err
	}
	f.recordProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestRevoke, cmd.ID, payloadHash)
	return nil
}

func (f *MetaFSM) applyProtocolCredentialMarkStale(payload []byte) error {
	cmd, err := decodeProtocolCredentialMarkStaleCmd(payload)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protocolCredentialStore == nil {
		return nil
	}
	payloadHash := protocolCredentialMarkStaleDigest(cmd)
	replay, err := f.checkProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestMarkStale, cmd.ID, payloadHash)
	if err != nil || replay {
		return err
	}
	_, err = f.protocolCredentialStore.ApplyMarkStale(cmd.ID, cmd.StaleAt, cmd.Reason)
	if err != nil {
		return err
	}
	f.recordProtocolCredentialRequestLocked(cmd.RequestID, protocolCredentialRequestMarkStale, cmd.ID, payloadHash)
	return nil
}

func (f *MetaFSM) applyProtocolCredentialLastUsed(payload []byte) error {
	cmd, err := decodeProtocolCredentialLastUsedCmd(payload)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protocolCredentialStore == nil {
		return nil
	}
	_, err = f.protocolCredentialStore.ApplyLastUsed(cmd.ID, cmd.LastUsedAt)
	return err
}

func (f *MetaFSM) checkProtocolCredentialRequestLocked(requestID, operation, credentialID string, payloadHash [sha256.Size]byte) (bool, error) {
	if requestID == "" || credentialID == "" {
		return false, protocred.ErrInvalid
	}
	if f.protocolCredentialRequests == nil {
		f.protocolCredentialRequests = make(map[string]ProtocolCredentialRequestRecord)
	}
	existing, ok := f.protocolCredentialRequests[requestID]
	if ok {
		if existing.Operation == operation && existing.CredentialID == credentialID && (existing.PayloadHash == payloadHash || existing.PayloadHash == [sha256.Size]byte{}) {
			return true, nil
		}
		return false, fmt.Errorf("%w: request_id %q already applied as %s for %s", protocred.ErrConflict, requestID, existing.Operation, existing.CredentialID)
	}
	return false, nil
}

func (f *MetaFSM) recordProtocolCredentialRequestLocked(requestID, operation, credentialID string, payloadHash [sha256.Size]byte) {
	if f.protocolCredentialRequests == nil {
		f.protocolCredentialRequests = make(map[string]ProtocolCredentialRequestRecord)
	}
	f.protocolCredentialRequests[requestID] = ProtocolCredentialRequestRecord{
		RequestID:    requestID,
		Operation:    operation,
		CredentialID: credentialID,
		PayloadHash:  payloadHash,
	}
}

func protocolCredentialCreateDigest(cmd ProtocolCredentialCreateCmd) [sha256.Size]byte {
	h := sha256.New()
	writeDigestString(h, protocolCredentialRequestCreate)
	writeDigestString(h, cmd.Credential.ID)
	writeDigestCredential(h, cmd.Credential)
	return sha256.Sum256(h.Sum(nil))
}

func protocolCredentialRotateDigest(cmd ProtocolCredentialRotateCmd) [sha256.Size]byte {
	h := sha256.New()
	writeDigestString(h, protocolCredentialRequestRotate)
	writeDigestString(h, cmd.ID)
	h.Write(cmd.SecretHash[:])
	writeDigestString(h, cmd.SecretHint)
	writeDigestInt64(h, unixNanos(cmd.RotatedAt))
	return sha256.Sum256(h.Sum(nil))
}

func protocolCredentialRevokeDigest(cmd ProtocolCredentialRevokeCmd) [sha256.Size]byte {
	h := sha256.New()
	writeDigestString(h, protocolCredentialRequestRevoke)
	writeDigestString(h, cmd.ID)
	writeDigestInt64(h, unixNanos(cmd.RevokedAt))
	return sha256.Sum256(h.Sum(nil))
}

func protocolCredentialMarkStaleDigest(cmd ProtocolCredentialMarkStaleCmd) [sha256.Size]byte {
	h := sha256.New()
	writeDigestString(h, protocolCredentialRequestMarkStale)
	writeDigestString(h, cmd.ID)
	writeDigestInt64(h, unixNanos(cmd.StaleAt))
	writeDigestString(h, cmd.Reason)
	return sha256.Sum256(h.Sum(nil))
}

func writeDigestCredential(h interface{ Write([]byte) (int, error) }, row protocred.Credential) {
	writeDigestString(h, row.ID)
	writeDigestString(h, row.SAID)
	writeDigestString(h, string(row.Protocol))
	writeDigestString(h, row.Resource)
	writeDigestString(h, string(row.Mode))
	h.Write(row.SecretHash[:])
	writeDigestString(h, row.SecretHint)
	writeDigestInt64(h, unixNanos(row.CreatedAt))
	writeDigestString(h, row.CreatedBy)
	writeDigestInt64(h, unixNanosPtr(row.ExpiresAt))
	writeDigestInt64(h, unixNanosPtr(row.RevokedAt))
	writeDigestInt64(h, unixNanosPtr(row.LastUsedAt))
	writeDigestUint64(h, row.Generation)
	writeDigestInt64(h, unixNanosPtr(row.StaleAt))
	writeDigestString(h, row.StaleReason)
}

func writeDigestString(h interface{ Write([]byte) (int, error) }, s string) {
	writeDigestUint64(h, uint64(len(s)))
	h.Write([]byte(s))
}

func writeDigestInt64(h interface{ Write([]byte) (int, error) }, n int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(n))
	h.Write(buf[:])
}

func writeDigestUint64(h interface{ Write([]byte) (int, error) }, n uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], n)
	h.Write(buf[:])
}
