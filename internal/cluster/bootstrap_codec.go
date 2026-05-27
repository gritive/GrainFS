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
	var gensOff flatbuffers.UOffsetT
	if len(gens) > 0 {
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
		gensOff = b.EndVector(len(genOffs))
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
	n := t.KekGenerationsLength()
	if n > 0 {
		gens = make([]KEKGen, n)
		var g clusterpb.KEKGen
		for i := 0; i < n; i++ {
			if !t.KekGenerations(&g, i) {
				continue
			}
			gens[i] = KEKGen{Gen: g.Gen(), Key: cloneBytes(g.KeyBytes())}
		}
	}
	return encKey, gens, psk, nil
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
