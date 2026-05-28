package cluster

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeBootstrapSecretsPayload encodes a legacy (pre-PR-2a) bootstrap secrets
// payload WITHOUT peer_spkis or cluster_key_dropped. Used ONLY by tests to
// produce legacy-format blobs for backward-compat assertions; moved out of
// production code (bootstrap_codec.go) because golangci-lint/unused flags
// functions that are only called from test files.
func encodeBootstrapSecretsPayload(encKey []byte, gens []KEKGen, psk []byte) []byte {
	b := clusterBuilderPool.Get()

	var encKeyOff flatbuffers.UOffsetT
	if len(encKey) > 0 {
		encKeyOff = b.CreateByteVector(encKey)
	}
	var pskOff flatbuffers.UOffsetT
	if len(psk) > 0 {
		pskOff = b.CreateByteVector(psk)
	}

	// Build child KEKGen tables BEFORE the parent Start (nested-vector rule).
	gensOff := buildKEKGensVector(b, gens)

	clusterpb.BootstrapSecretsPayloadStart(b)
	if encKeyOff != 0 {
		clusterpb.BootstrapSecretsPayloadAddEncryptionKey(b, encKeyOff)
	}
	if gensOff != 0 {
		clusterpb.BootstrapSecretsPayloadAddKekGenerations(b, gensOff)
	}
	if pskOff != 0 {
		clusterpb.BootstrapSecretsPayloadAddTransportPsk(b, pskOff)
	}
	return fbFinish(b, clusterpb.BootstrapSecretsPayloadEnd(b))
}
