package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestShardAADFields_DistinctPerShardIndex(t *testing.T) {
	cid := make([]byte, 16)
	a := encrypt.BuildAAD(encrypt.DomainShard, cid, ShardAADFields("b", "k", 0)...)
	c := encrypt.BuildAAD(encrypt.DomainShard, cid, ShardAADFields("b", "k", 1)...)
	if string(a) == string(c) {
		t.Fatal("shard AAD must differ by shard index")
	}
}
