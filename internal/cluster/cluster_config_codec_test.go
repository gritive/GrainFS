package cluster

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEncodeClusterConfigPatchCmd_Deterministic verifies that encoding the same
// patch twice with the same UpdatedAtUnixMs produces byte-identical output.
// This is the wire-level invariant that backs deterministic FSM Apply across
// replicas: every node that decodes a Raft log entry containing this patch
// must produce the same clusterConfigSnap.updatedAt — so the encoder must
// faithfully carry the proposer's stamped timestamp.
func TestEncodeClusterConfigPatchCmd_Deterministic(t *testing.T) {
	p := ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(25.0),
		AlertWebhook:                ptrString("https://hooks.example/a"),
		UpdatedAtUnixMs:             1715520000000,
	}

	a, err := EncodeClusterConfigPatchCmd(p)
	require.NoError(t, err)
	b, err := EncodeClusterConfigPatchCmd(p)
	require.NoError(t, err)

	require.True(t, bytes.Equal(a, b),
		"encoder must be deterministic for the same stamped patch; got len(a)=%d len(b)=%d", len(a), len(b))
}

// TestClusterConfigPatchCmd_UpdatedAt_RoundTrip ensures the proposer-stamped
// timestamp survives encode → decode unchanged. Combined with the FSM apply
// path using p.UpdatedAtUnixMs (instead of time.Now()), this gives every
// replica the same updatedAt for the same log entry.
func TestClusterConfigPatchCmd_UpdatedAt_RoundTrip(t *testing.T) {
	const stamp int64 = 1715520000000

	p := ClusterConfigPatch{
		AlertWebhook:    ptrString("https://hooks.example/a"),
		UpdatedAtUnixMs: stamp,
	}
	inner, err := EncodeClusterConfigPatchInner(p)
	require.NoError(t, err)
	got, err := DecodeClusterConfigPatchCmd(inner)
	require.NoError(t, err)
	require.Equal(t, stamp, got.UpdatedAtUnixMs)
}

// TestDecodeClusterConfigPatchCmd_SecretBytesIndependent verifies that
// DecodeClusterConfigPatchCmd copies AlertWebhookSecretWrapped out of the
// underlying FlatBuffer rather than aliasing it. The Raft log entry buffer
// may be pooled/reused after Apply commits; if the decode aliased the FB
// bytes, the in-memory clusterConfigSnap's ciphertext would mutate silently
// once the buffer was reused.
func TestDecodeClusterConfigPatchCmd_SecretBytesIndependent(t *testing.T) {
	secret := []byte("wrapped-ciphertext-bytes")
	p := ClusterConfigPatch{
		AlertWebhookSecretWrapped: secret,
		AlertWebhookSecretDEKGen:  9,
	}
	data, err := EncodeClusterConfigPatchInner(p)
	require.NoError(t, err)

	got, err := DecodeClusterConfigPatchCmd(data)
	require.NoError(t, err)
	require.Equal(t, secret, got.AlertWebhookSecretWrapped)
	require.Equal(t, uint32(9), got.AlertWebhookSecretDEKGen)

	// Mutate every byte of the source buffer to simulate a pooled/reused
	// Raft log entry. If Decode aliased the FB bytes, got's slice would
	// reflect this mutation.
	original := append([]byte(nil), got.AlertWebhookSecretWrapped...)
	for i := range data {
		data[i] ^= 0xFF
	}
	require.Equal(t, original, got.AlertWebhookSecretWrapped,
		"decoded secret must be independent of the source buffer")
}

func TestClusterConfigCodec_SerializeRoundTrip(t *testing.T) {
	c := NewClusterConfig()
	bc := 1.75
	c.applyPatch(ClusterConfigPatch{BoundedLoadsC: &bc}, time.UnixMilli(100))

	buf := serializeClusterConfig(c)
	snap, err := deserializeClusterConfig(buf)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if snap.boundedLoadsC == nil || *snap.boundedLoadsC != bc {
		t.Fatalf("boundedLoadsC roundtrip: %v", snap.boundedLoadsC)
	}
}

func TestClusterConfigCodec_BoundedLoadsRoundtrip(t *testing.T) {
	enabled := true
	cLowVal := 0.9
	cVal := 1.5
	ttl := 90 * time.Second
	patch := ClusterConfigPatch{
		WeightedHRWEnabled:      &enabled,
		BoundedLoadsEnabled:     &enabled,
		BoundedLoadsC:           &cVal,
		BoundedLoadsCLow:        &cLowVal,
		BoundedLoadsMaxStaleTTL: &ttl,
	}
	enc, err := EncodeClusterConfigPatchInner(patch)
	require.NoError(t, err)
	dec, err := DecodeClusterConfigPatchCmd(enc)
	require.NoError(t, err)
	require.NotNil(t, dec.WeightedHRWEnabled)
	require.Equal(t, true, *dec.WeightedHRWEnabled)
	require.NotNil(t, dec.BoundedLoadsEnabled)
	require.Equal(t, true, *dec.BoundedLoadsEnabled)
	require.NotNil(t, dec.BoundedLoadsC)
	require.Equal(t, 1.5, *dec.BoundedLoadsC)
	require.NotNil(t, dec.BoundedLoadsCLow)
	require.Equal(t, 0.9, *dec.BoundedLoadsCLow)
	require.NotNil(t, dec.BoundedLoadsMaxStaleTTL)
	require.Equal(t, 90*time.Second, *dec.BoundedLoadsMaxStaleTTL)
}
