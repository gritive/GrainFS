package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodeMetaJWTSigningKeyRotateCmd serializes a JWTSigningKeyRotate payload.
// kid, wrappedSecret, dekGen, and demotedAtUnix are all generated on the
// proposer side so the FSM apply path is fully deterministic across all nodes.
func EncodeMetaJWTSigningKeyRotateCmd(kid string, wrappedSecret []byte, dekGen uint32, demotedAtUnix int64) []byte {
	b := clusterBuilderPool.Get()

	kidOff := b.CreateByteString([]byte(kid))
	wrappedOff := b.CreateByteVector(wrappedSecret)

	clusterpb.MetaJWTSigningKeyRotateCmdStart(b)
	clusterpb.MetaJWTSigningKeyRotateCmdAddKid(b, kidOff)
	clusterpb.MetaJWTSigningKeyRotateCmdAddWrappedSecret(b, wrappedOff)
	clusterpb.MetaJWTSigningKeyRotateCmdAddDekGen(b, dekGen)
	clusterpb.MetaJWTSigningKeyRotateCmdAddDemotedAtUnix(b, demotedAtUnix)
	return fbFinish(b, clusterpb.MetaJWTSigningKeyRotateCmdEnd(b))
}

// decodeMetaJWTSigningKeyRotateCmd parses a JWTSigningKeyRotate payload.
func decodeMetaJWTSigningKeyRotateCmd(data []byte) (kid string, wrappedSecret []byte, dekGen uint32, demotedAtUnix int64, err error) {
	if len(data) == 0 {
		return "", nil, 0, 0, fmt.Errorf("jwt_meta_cmd_codec: JWTSigningKeyRotateCmd: empty payload")
	}
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaJWTSigningKeyRotateCmd {
		return clusterpb.GetRootAsMetaJWTSigningKeyRotateCmd(d, 0)
	})
	if err != nil {
		return "", nil, 0, 0, fmt.Errorf("jwt_meta_cmd_codec: JWTSigningKeyRotateCmd: %w", err)
	}
	kid = string(t.Kid())
	if kid == "" {
		return "", nil, 0, 0, fmt.Errorf("jwt_meta_cmd_codec: JWTSigningKeyRotateCmd: empty kid")
	}
	raw := t.WrappedSecretBytes()
	if len(raw) == 0 {
		return "", nil, 0, 0, fmt.Errorf("jwt_meta_cmd_codec: JWTSigningKeyRotateCmd: empty wrapped_secret")
	}
	wrappedSecret = append([]byte(nil), raw...)
	dekGen = t.DekGen()
	demotedAtUnix = t.DemotedAtUnix()
	return kid, wrappedSecret, dekGen, demotedAtUnix, nil
}

// EncodeMetaJWTSigningKeyPruneCmd serializes a JWTSigningKeyPrune payload.
// pruneAtUnix is the proposer's wall-clock unix-seconds used to evaluate
// PrunePrevSafe on every replica identically.
func EncodeMetaJWTSigningKeyPruneCmd(pruneAtUnix int64) []byte {
	b := clusterBuilderPool.Get()
	clusterpb.MetaJWTSigningKeyPruneCmdStart(b)
	clusterpb.MetaJWTSigningKeyPruneCmdAddPruneAtUnix(b, pruneAtUnix)
	return fbFinish(b, clusterpb.MetaJWTSigningKeyPruneCmdEnd(b))
}

// decodeMetaJWTSigningKeyPruneCmd parses a JWTSigningKeyPrune payload.
// Returns 0 on empty payload for backward compatibility with nil-payload
// prune commands (pre-fix Raft log entries).
func decodeMetaJWTSigningKeyPruneCmd(data []byte) (pruneAtUnix int64, err error) {
	if len(data) == 0 {
		// Backward-compat: old prune commands had nil payload; treat as "now"
		// is unsafe because it causes non-determinism — return a sentinel
		// that callers should reject as invalid.
		return 0, fmt.Errorf("jwt_meta_cmd_codec: JWTSigningKeyPruneCmd: empty payload (legacy nil-payload prune not supported)")
	}
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaJWTSigningKeyPruneCmd {
		return clusterpb.GetRootAsMetaJWTSigningKeyPruneCmd(d, 0)
	})
	if err != nil {
		return 0, fmt.Errorf("jwt_meta_cmd_codec: JWTSigningKeyPruneCmd: %w", err)
	}
	return t.PruneAtUnix(), nil
}
