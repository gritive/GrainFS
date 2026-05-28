package cluster

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// KEKGen is a single KEK generation: a generation number plus its raw key
// bytes. It is the Go-side mirror of clusterpb.KEKGen, used by the bootstrap
// secrets codec.
type KEKGen struct {
	Gen uint32
	Key []byte
}

// encodeBootstrapSecretsPayload serializes the INNER plaintext that Phase-1
// seals via encrypt.SealToPeer (the joiner OpenFromPeer-s + decodes it). All
// fields are optional; empty slices encode as absent vectors.
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

func decodeBootstrapSecretsPayload(data []byte) (encKey []byte, gens []KEKGen, psk []byte, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.BootstrapSecretsPayload {
		return clusterpb.GetRootAsBootstrapSecretsPayload(d, 0)
	})
	if e != nil {
		return nil, nil, nil, e
	}
	encKey = cloneBytes(t.EncryptionKeyBytes())
	psk = cloneBytes(t.TransportPskBytes())
	gens = decodeKEKGens(t)
	return encKey, gens, psk, nil
}

// buildKEKGensVector builds the nested KEKGen vector and returns its offset, or
// 0 when there are no generations. MUST be called BEFORE the parent table Start
// (FlatBuffers nested-vector rule).
func buildKEKGensVector(b *flatbuffers.Builder, gens []KEKGen) flatbuffers.UOffsetT {
	if len(gens) == 0 {
		return 0
	}
	genOffs := make([]flatbuffers.UOffsetT, len(gens))
	for i, g := range gens {
		var keyOff flatbuffers.UOffsetT
		if len(g.Key) > 0 {
			keyOff = b.CreateByteVector(g.Key)
		}
		clusterpb.KEKGenStart(b)
		clusterpb.KEKGenAddGen(b, g.Gen)
		if keyOff != 0 {
			clusterpb.KEKGenAddKey(b, keyOff)
		}
		genOffs[i] = clusterpb.KEKGenEnd(b)
	}
	clusterpb.BootstrapSecretsPayloadStartKekGenerationsVector(b, len(genOffs))
	for i := len(genOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(genOffs[i])
	}
	return b.EndVector(len(genOffs))
}

// decodeKEKGens extracts the KEKGen vector from a decoded payload table.
func decodeKEKGens(t *clusterpb.BootstrapSecretsPayload) []KEKGen {
	n := t.KekGenerationsLength()
	if n == 0 {
		return nil
	}
	gens := make([]KEKGen, n)
	var g clusterpb.KEKGen
	for i := 0; i < n; i++ {
		if !t.KekGenerations(&g, i) {
			continue
		}
		gens[i] = KEKGen{Gen: g.Gen(), Key: cloneBytes(g.KeyBytes())}
	}
	return gens
}

// EncodeBootstrapSecretsPayloadWithCutover serializes the INNER plaintext plus
// the zero-CA cutover wire fields (member per-node SPKI set + cluster_key_dropped
// bit). PR-1 only wires the fields; the post-cutover joiner consumes them in PR-2.
func EncodeBootstrapSecretsPayloadWithCutover(encKey []byte, gens []KEKGen, psk []byte, peerSPKIs [][32]byte, clusterKeyDropped bool) []byte {
	b := clusterBuilderPool.Get()
	var encKeyOff, pskOff flatbuffers.UOffsetT
	if len(encKey) > 0 {
		encKeyOff = b.CreateByteVector(encKey)
	}
	if len(psk) > 0 {
		pskOff = b.CreateByteVector(psk)
	}
	gensOff := buildKEKGensVector(b, gens)

	var spkisOff flatbuffers.UOffsetT
	if len(peerSPKIs) > 0 {
		offs := make([]flatbuffers.UOffsetT, len(peerSPKIs))
		for i := range peerSPKIs {
			vec := b.CreateByteVector(peerSPKIs[i][:])
			clusterpb.SPKIBytesStart(b)
			clusterpb.SPKIBytesAddValue(b, vec)
			offs[i] = clusterpb.SPKIBytesEnd(b)
		}
		clusterpb.BootstrapSecretsPayloadStartPeerSpkisVector(b, len(offs))
		for i := len(offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offs[i])
		}
		spkisOff = b.EndVector(len(offs))
	}

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
	if spkisOff != 0 {
		clusterpb.BootstrapSecretsPayloadAddPeerSpkis(b, spkisOff)
	}
	clusterpb.BootstrapSecretsPayloadAddClusterKeyDropped(b, clusterKeyDropped)
	return fbFinish(b, clusterpb.BootstrapSecretsPayloadEnd(b))
}

// DecodeBootstrapSecretsPayloadWithCutover mirrors EncodeBootstrapSecretsPayloadWithCutover.
// Legacy payloads (no cutover fields) decode cleanly: empty SPKI set + false.
func DecodeBootstrapSecretsPayloadWithCutover(data []byte) (encKey []byte, gens []KEKGen, psk []byte, peerSPKIs [][32]byte, clusterKeyDropped bool, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.BootstrapSecretsPayload {
		return clusterpb.GetRootAsBootstrapSecretsPayload(d, 0)
	})
	if e != nil {
		return nil, nil, nil, nil, false, e
	}
	encKey = cloneBytes(t.EncryptionKeyBytes())
	psk = cloneBytes(t.TransportPskBytes())
	gens = decodeKEKGens(t)
	var sb clusterpb.SPKIBytes
	for i := 0; i < t.PeerSpkisLength(); i++ {
		if !t.PeerSpkis(&sb, i) {
			continue
		}
		if raw := sb.ValueBytes(); len(raw) == 32 {
			var s [32]byte
			copy(s[:], raw)
			peerSPKIs = append(peerSPKIs, s)
		}
	}
	clusterKeyDropped = t.ClusterKeyDropped()
	return encKey, gens, psk, peerSPKIs, clusterKeyDropped, nil
}

// DecodeBootstrapSecretsPayload is the exported wrapper the W9b joiner uses to
// decode the INNER plaintext after OpenFromPeer. Mirrors the leader-side encode
// (encodeBootstrapSecretsPayload).
func DecodeBootstrapSecretsPayload(data []byte) (encKey []byte, gens []KEKGen, psk []byte, err error) {
	return decodeBootstrapSecretsPayload(data)
}

// DecodeSealedBootstrap is the exported wrapper the W9b joiner uses to decode
// the outer SealedToPeer envelope carried in JoinReply.sealed_bootstrap.
func DecodeSealedBootstrap(data []byte) (ephemeralPub, ciphertext []byte, err error) {
	return decodeSealedBootstrap(data)
}

// encodeSealedBootstrap serializes the outer envelope (wire form of
// encrypt.SealedToPeer) carried in JoinReply.sealed_bootstrap.
func encodeSealedBootstrap(ephemeralPub, ciphertext []byte) []byte {
	b := clusterBuilderPool.Get()
	var ephOff flatbuffers.UOffsetT
	if len(ephemeralPub) > 0 {
		ephOff = b.CreateByteVector(ephemeralPub)
	}
	var ctOff flatbuffers.UOffsetT
	if len(ciphertext) > 0 {
		ctOff = b.CreateByteVector(ciphertext)
	}
	clusterpb.SealedBootstrapStart(b)
	if ephOff != 0 {
		clusterpb.SealedBootstrapAddEphemeralPub(b, ephOff)
	}
	if ctOff != 0 {
		clusterpb.SealedBootstrapAddCiphertext(b, ctOff)
	}
	return fbFinish(b, clusterpb.SealedBootstrapEnd(b))
}

func decodeSealedBootstrap(data []byte) (ephemeralPub, ciphertext []byte, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.SealedBootstrap {
		return clusterpb.GetRootAsSealedBootstrap(d, 0)
	})
	if e != nil {
		return nil, nil, e
	}
	return cloneBytes(t.EphemeralPubBytes()), cloneBytes(t.CiphertextBytes()), nil
}

// cloneBytes returns a copy of b, or nil if b is empty. FlatBuffers accessor
// byte slices alias the backing buffer; decoders must copy before the buffer
// is reused/pooled.
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
