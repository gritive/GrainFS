package cluster

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/protocred"
)

type ProtocolCredentialCreateCmd struct {
	RequestID  string
	Credential protocred.Credential
}

type ProtocolCredentialRotateCmd struct {
	RequestID  string
	ID         string
	SecretHash [sha256.Size]byte
	SecretHint string
	RotatedAt  time.Time
	SecretEnc  []byte
}

type ProtocolCredentialRevokeCmd struct {
	RequestID string
	ID        string
	RevokedAt time.Time
}

type ProtocolCredentialMarkStaleCmd struct {
	RequestID string
	ID        string
	StaleAt   time.Time
	Reason    string
}

type ProtocolCredentialLastUsedCmd struct {
	ID         string
	LastUsedAt time.Time
}

type ProtocolCredentialRequestRecord struct {
	RequestID    string
	Operation    string
	CredentialID string
	PayloadHash  [sha256.Size]byte
}

func encodeProtocolCredentialCreateCmd(cmd ProtocolCredentialCreateCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(cmd.RequestID)
	rowOff := buildProtocolCredentialEntry(b, cmd.Credential)
	clusterpb.MetaProtocolCredentialCreateCmdStart(b)
	clusterpb.MetaProtocolCredentialCreateCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaProtocolCredentialCreateCmdAddRow(b, rowOff)
	return fbFinish(b, clusterpb.MetaProtocolCredentialCreateCmdEnd(b)), nil
}

func decodeProtocolCredentialCreateCmd(data []byte) (ProtocolCredentialCreateCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaProtocolCredentialCreateCmd {
		return clusterpb.GetRootAsMetaProtocolCredentialCreateCmd(d, 0)
	})
	if err != nil {
		return ProtocolCredentialCreateCmd{}, fmt.Errorf("protocol_credentials_codec: create: %w", err)
	}
	row := t.Row(nil)
	if row == nil {
		return ProtocolCredentialCreateCmd{}, fmt.Errorf("protocol_credentials_codec: create: missing row")
	}
	cred, err := decodeProtocolCredentialEntry(row)
	if err != nil {
		return ProtocolCredentialCreateCmd{}, err
	}
	return ProtocolCredentialCreateCmd{
		RequestID:  string(t.RequestId()),
		Credential: cred,
	}, nil
}

func encodeProtocolCredentialRotateCmd(cmd ProtocolCredentialRotateCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(cmd.RequestID)
	idOff := b.CreateString(cmd.ID)
	hashOff := b.CreateByteVector(cmd.SecretHash[:])
	hintOff := b.CreateString(cmd.SecretHint)
	encOff := b.CreateByteVector(cmd.SecretEnc)
	clusterpb.MetaProtocolCredentialRotateCmdStart(b)
	clusterpb.MetaProtocolCredentialRotateCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaProtocolCredentialRotateCmdAddId(b, idOff)
	clusterpb.MetaProtocolCredentialRotateCmdAddSecretHash(b, hashOff)
	clusterpb.MetaProtocolCredentialRotateCmdAddSecretHint(b, hintOff)
	clusterpb.MetaProtocolCredentialRotateCmdAddRotatedAtUnixNanos(b, unixNanos(cmd.RotatedAt))
	if len(cmd.SecretEnc) > 0 {
		clusterpb.MetaProtocolCredentialRotateCmdAddSecretEnc(b, encOff)
	}
	return fbFinish(b, clusterpb.MetaProtocolCredentialRotateCmdEnd(b)), nil
}

func decodeProtocolCredentialRotateCmd(data []byte) (ProtocolCredentialRotateCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaProtocolCredentialRotateCmd {
		return clusterpb.GetRootAsMetaProtocolCredentialRotateCmd(d, 0)
	})
	if err != nil {
		return ProtocolCredentialRotateCmd{}, fmt.Errorf("protocol_credentials_codec: rotate: %w", err)
	}
	hash, err := decodeSecretHash(t.SecretHashBytes())
	if err != nil {
		return ProtocolCredentialRotateCmd{}, err
	}
	return ProtocolCredentialRotateCmd{
		RequestID:  string(t.RequestId()),
		ID:         string(t.Id()),
		SecretHash: hash,
		SecretHint: string(t.SecretHint()),
		RotatedAt:  timeFromUnixNanos(t.RotatedAtUnixNanos()),
		SecretEnc:  cloneBytes(t.SecretEncBytes()),
	}, nil
}

func encodeProtocolCredentialRevokeCmd(cmd ProtocolCredentialRevokeCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(cmd.RequestID)
	idOff := b.CreateString(cmd.ID)
	clusterpb.MetaProtocolCredentialRevokeCmdStart(b)
	clusterpb.MetaProtocolCredentialRevokeCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaProtocolCredentialRevokeCmdAddId(b, idOff)
	clusterpb.MetaProtocolCredentialRevokeCmdAddRevokedAtUnixNanos(b, unixNanos(cmd.RevokedAt))
	return fbFinish(b, clusterpb.MetaProtocolCredentialRevokeCmdEnd(b)), nil
}

func decodeProtocolCredentialRevokeCmd(data []byte) (ProtocolCredentialRevokeCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaProtocolCredentialRevokeCmd {
		return clusterpb.GetRootAsMetaProtocolCredentialRevokeCmd(d, 0)
	})
	if err != nil {
		return ProtocolCredentialRevokeCmd{}, fmt.Errorf("protocol_credentials_codec: revoke: %w", err)
	}
	return ProtocolCredentialRevokeCmd{
		RequestID: string(t.RequestId()),
		ID:        string(t.Id()),
		RevokedAt: timeFromUnixNanos(t.RevokedAtUnixNanos()),
	}, nil
}

// nolint:unused // MarkStale/LastUsed decode+apply are live; the propose-side
// service wiring (encode caller) lands in a protocol-credential follow-up. Tests
// exercise this encoder today.
func encodeProtocolCredentialMarkStaleCmd(cmd ProtocolCredentialMarkStaleCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	requestIDOff := b.CreateString(cmd.RequestID)
	idOff := b.CreateString(cmd.ID)
	reasonOff := b.CreateString(cmd.Reason)
	clusterpb.MetaProtocolCredentialMarkStaleCmdStart(b)
	clusterpb.MetaProtocolCredentialMarkStaleCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaProtocolCredentialMarkStaleCmdAddId(b, idOff)
	clusterpb.MetaProtocolCredentialMarkStaleCmdAddStaleAtUnixNanos(b, unixNanos(cmd.StaleAt))
	clusterpb.MetaProtocolCredentialMarkStaleCmdAddReason(b, reasonOff)
	return fbFinish(b, clusterpb.MetaProtocolCredentialMarkStaleCmdEnd(b)), nil
}

func decodeProtocolCredentialMarkStaleCmd(data []byte) (ProtocolCredentialMarkStaleCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaProtocolCredentialMarkStaleCmd {
		return clusterpb.GetRootAsMetaProtocolCredentialMarkStaleCmd(d, 0)
	})
	if err != nil {
		return ProtocolCredentialMarkStaleCmd{}, fmt.Errorf("protocol_credentials_codec: mark stale: %w", err)
	}
	return ProtocolCredentialMarkStaleCmd{
		RequestID: string(t.RequestId()),
		ID:        string(t.Id()),
		StaleAt:   timeFromUnixNanos(t.StaleAtUnixNanos()),
		Reason:    string(t.Reason()),
	}, nil
}

// nolint:unused // see encodeProtocolCredentialMarkStaleCmd — propose-side
// service wiring lands in a protocol-credential follow-up; tests exercise it.
func encodeProtocolCredentialLastUsedCmd(cmd ProtocolCredentialLastUsedCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(cmd.ID)
	clusterpb.MetaProtocolCredentialLastUsedCmdStart(b)
	clusterpb.MetaProtocolCredentialLastUsedCmdAddId(b, idOff)
	clusterpb.MetaProtocolCredentialLastUsedCmdAddLastUsedAtUnixNanos(b, unixNanos(cmd.LastUsedAt))
	return fbFinish(b, clusterpb.MetaProtocolCredentialLastUsedCmdEnd(b)), nil
}

func decodeProtocolCredentialLastUsedCmd(data []byte) (ProtocolCredentialLastUsedCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaProtocolCredentialLastUsedCmd {
		return clusterpb.GetRootAsMetaProtocolCredentialLastUsedCmd(d, 0)
	})
	if err != nil {
		return ProtocolCredentialLastUsedCmd{}, fmt.Errorf("protocol_credentials_codec: last used: %w", err)
	}
	return ProtocolCredentialLastUsedCmd{
		ID:         string(t.Id()),
		LastUsedAt: timeFromUnixNanos(t.LastUsedAtUnixNanos()),
	}, nil
}

func encodeProtocolCredentialsSnapshotState(rows []protocred.Credential, requests []ProtocolCredentialRequestRecord) ([]byte, error) {
	b := clusterBuilderPool.Get()
	sorted := append([]protocred.Credential(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	sortedRequests := append([]ProtocolCredentialRequestRecord(nil), requests...)
	sort.Slice(sortedRequests, func(i, j int) bool {
		return sortedRequests[i].RequestID < sortedRequests[j].RequestID
	})

	rowOffs := make([]flatbuffers.UOffsetT, len(sorted))
	for i := len(sorted) - 1; i >= 0; i-- {
		rowOffs[i] = buildProtocolCredentialEntry(b, sorted[i])
	}
	clusterpb.MetaProtocolCredentialsSnapshotStartRowsVector(b, len(rowOffs))
	for i := len(rowOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(rowOffs[i])
	}
	rowsVec := b.EndVector(len(rowOffs))

	requestOffs := make([]flatbuffers.UOffsetT, len(sortedRequests))
	for i := len(sortedRequests) - 1; i >= 0; i-- {
		requestOffs[i] = buildProtocolCredentialRequestEntry(b, sortedRequests[i])
	}
	clusterpb.MetaProtocolCredentialsSnapshotStartRequestsVector(b, len(requestOffs))
	for i := len(requestOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(requestOffs[i])
	}
	requestsVec := b.EndVector(len(requestOffs))

	clusterpb.MetaProtocolCredentialsSnapshotStart(b)
	clusterpb.MetaProtocolCredentialsSnapshotAddRows(b, rowsVec)
	clusterpb.MetaProtocolCredentialsSnapshotAddRequests(b, requestsVec)
	return fbFinish(b, clusterpb.MetaProtocolCredentialsSnapshotEnd(b)), nil
}

func decodeProtocolCredentialsSnapshotState(data []byte) ([]protocred.Credential, []ProtocolCredentialRequestRecord, error) {
	snap, err := fbSafe(data, func(d []byte) *clusterpb.MetaProtocolCredentialsSnapshot {
		return clusterpb.GetRootAsMetaProtocolCredentialsSnapshot(d, 0)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("protocol_credentials_codec: snapshot: %w", err)
	}
	rows := make([]protocred.Credential, snap.RowsLength())
	var rowFB clusterpb.MetaProtocolCredentialEntry
	for i := 0; i < snap.RowsLength(); i++ {
		if !snap.Rows(&rowFB, i) {
			return nil, nil, fmt.Errorf("protocol_credentials_codec: snapshot row %d decode failed", i)
		}
		row, err := decodeProtocolCredentialEntry(&rowFB)
		if err != nil {
			return nil, nil, err
		}
		rows[i] = row
	}
	requests := make([]ProtocolCredentialRequestRecord, snap.RequestsLength())
	var reqFB clusterpb.MetaProtocolCredentialRequestEntry
	for i := 0; i < snap.RequestsLength(); i++ {
		if !snap.Requests(&reqFB, i) {
			return nil, nil, fmt.Errorf("protocol_credentials_codec: snapshot request %d decode failed", i)
		}
		requests[i] = ProtocolCredentialRequestRecord{
			RequestID:    string(reqFB.RequestId()),
			Operation:    string(reqFB.Operation()),
			CredentialID: string(reqFB.CredentialId()),
		}
		if rawHash := reqFB.PayloadHashBytes(); len(rawHash) > 0 {
			hash, err := decodeSecretHash(rawHash)
			if err != nil {
				return nil, nil, fmt.Errorf("protocol_credentials_codec: snapshot request %d: %w", i, err)
			}
			requests[i].PayloadHash = hash
		}
		if err := validateProtocolCredentialRequestRecord(requests[i]); err != nil {
			return nil, nil, fmt.Errorf("protocol_credentials_codec: snapshot request %d: %w", i, err)
		}
	}
	return rows, requests, nil
}

func buildProtocolCredentialEntry(b *flatbuffers.Builder, row protocred.Credential) flatbuffers.UOffsetT {
	idOff := b.CreateString(row.ID)
	saOff := b.CreateString(row.SAID)
	protoOff := b.CreateString(string(row.Protocol))
	resourceOff := b.CreateString(row.Resource)
	modeOff := b.CreateString(string(row.Mode))
	hashOff := b.CreateByteVector(row.SecretHash[:])
	hintOff := b.CreateString(row.SecretHint)
	encOff := b.CreateByteVector(row.SecretEnc)
	createdByOff := b.CreateString(row.CreatedBy)
	staleReasonOff := b.CreateString(row.StaleReason)

	clusterpb.MetaProtocolCredentialEntryStart(b)
	clusterpb.MetaProtocolCredentialEntryAddId(b, idOff)
	clusterpb.MetaProtocolCredentialEntryAddSaId(b, saOff)
	clusterpb.MetaProtocolCredentialEntryAddProtocol(b, protoOff)
	clusterpb.MetaProtocolCredentialEntryAddResource(b, resourceOff)
	clusterpb.MetaProtocolCredentialEntryAddMode(b, modeOff)
	clusterpb.MetaProtocolCredentialEntryAddSecretHash(b, hashOff)
	clusterpb.MetaProtocolCredentialEntryAddSecretHint(b, hintOff)
	clusterpb.MetaProtocolCredentialEntryAddCreatedAtUnixNanos(b, unixNanos(row.CreatedAt))
	clusterpb.MetaProtocolCredentialEntryAddCreatedBy(b, createdByOff)
	clusterpb.MetaProtocolCredentialEntryAddExpiresAtUnixNanos(b, unixNanosPtr(row.ExpiresAt))
	clusterpb.MetaProtocolCredentialEntryAddRevokedAtUnixNanos(b, unixNanosPtr(row.RevokedAt))
	clusterpb.MetaProtocolCredentialEntryAddLastUsedAtUnixNanos(b, unixNanosPtr(row.LastUsedAt))
	clusterpb.MetaProtocolCredentialEntryAddGeneration(b, row.Generation)
	clusterpb.MetaProtocolCredentialEntryAddStaleAtUnixNanos(b, unixNanosPtr(row.StaleAt))
	clusterpb.MetaProtocolCredentialEntryAddStaleReason(b, staleReasonOff)
	if len(row.SecretEnc) > 0 {
		clusterpb.MetaProtocolCredentialEntryAddSecretEnc(b, encOff)
	}
	return clusterpb.MetaProtocolCredentialEntryEnd(b)
}

func decodeProtocolCredentialEntry(row *clusterpb.MetaProtocolCredentialEntry) (protocred.Credential, error) {
	hash, err := decodeSecretHash(row.SecretHashBytes())
	if err != nil {
		return protocred.Credential{}, err
	}
	cred := protocred.Credential{
		ID:          string(row.Id()),
		SAID:        string(row.SaId()),
		Protocol:    protocred.Protocol(row.Protocol()),
		Resource:    string(row.Resource()),
		Mode:        protocred.Mode(row.Mode()),
		SecretHash:  hash,
		SecretHint:  string(row.SecretHint()),
		CreatedAt:   timeFromUnixNanos(row.CreatedAtUnixNanos()),
		CreatedBy:   string(row.CreatedBy()),
		ExpiresAt:   timePtrFromUnixNanos(row.ExpiresAtUnixNanos()),
		RevokedAt:   timePtrFromUnixNanos(row.RevokedAtUnixNanos()),
		LastUsedAt:  timePtrFromUnixNanos(row.LastUsedAtUnixNanos()),
		Generation:  row.Generation(),
		StaleAt:     timePtrFromUnixNanos(row.StaleAtUnixNanos()),
		StaleReason: string(row.StaleReason()),
		SecretEnc:   cloneBytes(row.SecretEncBytes()),
	}
	if cred.Generation == 0 {
		cred.Generation = 1
	}
	return cred, nil
}

func buildProtocolCredentialRequestEntry(b *flatbuffers.Builder, row ProtocolCredentialRequestRecord) flatbuffers.UOffsetT {
	requestIDOff := b.CreateString(row.RequestID)
	operationOff := b.CreateString(row.Operation)
	credentialIDOff := b.CreateString(row.CredentialID)
	payloadHashOff := b.CreateByteVector(row.PayloadHash[:])
	clusterpb.MetaProtocolCredentialRequestEntryStart(b)
	clusterpb.MetaProtocolCredentialRequestEntryAddRequestId(b, requestIDOff)
	clusterpb.MetaProtocolCredentialRequestEntryAddOperation(b, operationOff)
	clusterpb.MetaProtocolCredentialRequestEntryAddCredentialId(b, credentialIDOff)
	clusterpb.MetaProtocolCredentialRequestEntryAddPayloadHash(b, payloadHashOff)
	return clusterpb.MetaProtocolCredentialRequestEntryEnd(b)
}

func validateProtocolCredentialRequestRecord(row ProtocolCredentialRequestRecord) error {
	if row.RequestID == "" || row.CredentialID == "" {
		return fmt.Errorf("invalid request record")
	}
	switch row.Operation {
	case protocolCredentialRequestCreate, protocolCredentialRequestRotate, protocolCredentialRequestRevoke, protocolCredentialRequestMarkStale:
		return nil
	default:
		return fmt.Errorf("invalid request operation %q", row.Operation)
	}
}

func decodeSecretHash(raw []byte) ([sha256.Size]byte, error) {
	var out [sha256.Size]byte
	if len(raw) != sha256.Size {
		return out, fmt.Errorf("protocol_credentials_codec: secret_hash len = %d, want %d", len(raw), sha256.Size)
	}
	copy(out[:], raw)
	return out, nil
}

func unixNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func unixNanosPtr(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return unixNanos(*t)
}

func timeFromUnixNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func timePtrFromUnixNanos(n int64) *time.Time {
	if n == 0 {
		return nil
	}
	t := timeFromUnixNanos(n)
	return &t
}
