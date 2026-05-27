package storage

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestSegmentFileAADFields_DistinctPerSegment(t *testing.T) {
	a := segmentFileAADFields("b", "k", "blob-1", 0)
	c := segmentFileAADFields("b", "k", "blob-2", 0)
	cid := make([]byte, 16)
	aadA := encrypt.BuildAAD(encrypt.DomainShard, cid, a...)
	aadC := encrypt.BuildAAD(encrypt.DomainShard, cid, c...)
	if string(aadA) == string(aadC) {
		t.Fatal("segment AAD must differ by blobID")
	}
}

func TestObjectFileAADFields_ChunkOrdinalBinds(t *testing.T) {
	cid := make([]byte, 16)
	a0 := encrypt.BuildAAD(encrypt.DomainShard, cid, objectFileAADFields("b", "k", 0)...)
	a1 := encrypt.BuildAAD(encrypt.DomainShard, cid, objectFileAADFields("b", "k", 1)...)
	if string(a0) == string(a1) {
		t.Fatal("object AAD must differ by chunk ordinal")
	}
}
