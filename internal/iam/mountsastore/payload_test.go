package mountsastore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeCreate(t *testing.T) {
	sa := MountSA{
		Name:       "nfsmount-alice",
		NumericUID: 1001,
		CreatedAt:  time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC).UnixNano(),
		CreatedBy:  "admin",
	}
	buf := EncodeCreatePayload(sa)
	got, err := DecodeCreatePayload(buf)
	require.NoError(t, err)
	require.Equal(t, sa, got)
}

func TestEncodeDecodeDelete(t *testing.T) {
	buf := EncodeDeletePayload("nfsmount-bob")
	name, err := DecodeDeletePayload(buf)
	require.NoError(t, err)
	require.Equal(t, "nfsmount-bob", name)
}

func TestEncodeDecodeAttachPolicy(t *testing.T) {
	buf := EncodeAttachPolicyPayload("nfsmount-alice", "NFSMountOnly")
	mountSA, policy, err := DecodeAttachPolicyPayload(buf)
	require.NoError(t, err)
	require.Equal(t, "nfsmount-alice", mountSA)
	require.Equal(t, "NFSMountOnly", policy)
}

func TestEncodeDecodeDetachPolicy(t *testing.T) {
	buf := EncodeDetachPolicyPayload("nfsmount-alice", "NFSMountOnly")
	mountSA, policy, err := DecodeDetachPolicyPayload(buf)
	require.NoError(t, err)
	require.Equal(t, "nfsmount-alice", mountSA)
	require.Equal(t, "NFSMountOnly", policy)
}

func TestDecodeCreateEmptyBuf_Error(t *testing.T) {
	_, err := DecodeCreatePayload(nil)
	require.Error(t, err)
	_, err = DecodeCreatePayload([]byte{})
	require.Error(t, err)
}

func TestDecodeDeleteEmptyBuf_Error(t *testing.T) {
	_, err := DecodeDeletePayload(nil)
	require.Error(t, err)
	_, err = DecodeDeletePayload([]byte{})
	require.Error(t, err)
}

func TestDecodeAttachPolicyEmptyBuf_Error(t *testing.T) {
	_, _, err := DecodeAttachPolicyPayload(nil)
	require.Error(t, err)
	_, _, err = DecodeAttachPolicyPayload([]byte{})
	require.Error(t, err)
}

func TestDecodeDetachPolicyEmptyBuf_Error(t *testing.T) {
	_, _, err := DecodeDetachPolicyPayload(nil)
	require.Error(t, err)
	_, _, err = DecodeDetachPolicyPayload([]byte{})
	require.Error(t, err)
}
