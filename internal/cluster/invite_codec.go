package cluster

import (
	"crypto/ed25519"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeInviteMintCmd serializes an InviteMint payload (raft cmd data — wrap
// with encodeMetaCmd to get the MetaCmd envelope).
func encodeInviteMintCmd(id string, pub ed25519.PublicKey, expiryNanos int64) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(id)
	pubOff := b.CreateByteVector(pub)
	clusterpb.MetaInviteMintCmdStart(b)
	clusterpb.MetaInviteMintCmdAddId(b, idOff)
	clusterpb.MetaInviteMintCmdAddPub(b, pubOff)
	clusterpb.MetaInviteMintCmdAddExpiryNanos(b, expiryNanos)
	return fbFinish(b, clusterpb.MetaInviteMintCmdEnd(b)), nil
}

func decodeInviteMintCmd(data []byte) (id string, pub ed25519.PublicKey, expiryNanos int64, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.MetaInviteMintCmd {
		return clusterpb.GetRootAsMetaInviteMintCmd(d, 0)
	})
	if e != nil {
		return "", nil, 0, e
	}
	rawPub := t.PubBytes()
	if len(rawPub) != ed25519.PublicKeySize {
		return "", nil, 0, fmt.Errorf("invite_codec: pub must be %d bytes, got %d", ed25519.PublicKeySize, len(rawPub))
	}
	pub = append(ed25519.PublicKey(nil), rawPub...)
	return string(t.Id()), pub, t.ExpiryNanos(), nil
}

// encodeInviteConsumeCmd serializes an InviteConsume payload.
func encodeInviteConsumeCmd(id string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(id)
	clusterpb.MetaInviteConsumeCmdStart(b)
	clusterpb.MetaInviteConsumeCmdAddId(b, idOff)
	return fbFinish(b, clusterpb.MetaInviteConsumeCmdEnd(b)), nil
}

func decodeInviteConsumeCmd(data []byte) (id string, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.MetaInviteConsumeCmd {
		return clusterpb.GetRootAsMetaInviteConsumeCmd(d, 0)
	})
	if e != nil {
		return "", e
	}
	return string(t.Id()), nil
}
