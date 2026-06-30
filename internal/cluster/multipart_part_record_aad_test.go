package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestPartRecordAADFieldsDistinct(t *testing.T) {
	var clusterID [16]byte
	a := encrypt.BuildAAD(encrypt.DomainSpool, clusterID[:], partRecordAADFieldsInto(nil, "cluster-part:1", 0)...)
	b := encrypt.BuildAAD(encrypt.DomainSpool, clusterID[:], partRecordAADFieldsInto(nil, "cluster-part:2", 0)...)
	require.NotEqual(t, a, b, "different part record domains must produce different AAD")
	// Record index is bound: same domain, different record → different AAD
	// (prevents intra-part record frame reorder/splice).
	r := encrypt.BuildAAD(encrypt.DomainSpool, clusterID[:], partRecordAADFieldsInto(nil, "cluster-part:1", 1)...)
	require.NotEqual(t, a, r, "record index must be bound into the AAD")
	// DomainSpool must differ from DomainShard for the same fields.
	s := encrypt.BuildAAD(encrypt.DomainShard, clusterID[:], partRecordAADFieldsInto(nil, "cluster-part:1", 0)...)
	require.NotEqual(t, a, s, "DomainSpool must not collide with DomainShard")
}
