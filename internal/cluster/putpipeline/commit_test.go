package putpipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestCommitCoord_EarlyAckAtKDataShards(t *testing.T) {
	in := make(chan ShardWriteResult, 4)
	meta := make(chan MetadataRecord, 1)
	c := &CommitCoord{in: in, metaBatchCh: meta, waiters: make(map[uint64]*putWaiter)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	early := make(chan error, 1)
	final := make(chan error, 1)
	c.registerPut(42, &putWaiter{
		shardsTotal: 4,
		cfg:         cluster.ECConfig{DataShards: 2, ParityShards: 2},
		earlyAck:    early,
		finalDone:   final,
		metadata:    MetadataRecord{Bucket: "b", Key: "k"},
	})

	in <- ShardWriteResult{PutID: 42, ShardIdx: 0, Bytes: 100}
	in <- ShardWriteResult{PutID: 42, ShardIdx: 1, Bytes: 100}
	select {
	case err := <-early:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("earlyAck not fired after 2 data shards")
	}
	select {
	case <-final:
		t.Fatal("finalDone fired prematurely")
	case <-time.After(50 * time.Millisecond):
	}
	in <- ShardWriteResult{PutID: 42, ShardIdx: 2, Bytes: 100}
	in <- ShardWriteResult{PutID: 42, ShardIdx: 3, Bytes: 100}
	select {
	case err := <-final:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("finalDone not fired after all 4 shards")
	}
	select {
	case rec := <-meta:
		require.Equal(t, "b", rec.Bucket)
		require.Equal(t, "k", rec.Key)
	default:
		t.Fatal("metadata record not queued")
	}
}

func TestCommitCoord_FailsFastWhenKUnreachable(t *testing.T) {
	in := make(chan ShardWriteResult, 4)
	meta := make(chan MetadataRecord, 1)
	c := &CommitCoord{in: in, metaBatchCh: meta, waiters: make(map[uint64]*putWaiter)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	early := make(chan error, 1)
	final := make(chan error, 1)
	c.registerPut(99, &putWaiter{
		shardsTotal: 4,
		cfg:         cluster.ECConfig{DataShards: 2, ParityShards: 2},
		earlyAck:    early,
		finalDone:   final,
		metadata:    MetadataRecord{Bucket: "b", Key: "k"},
	})

	// Both data shards fail; parities succeed. dataShardsOK never
	// reaches 2 → earlyAck must report failure, finalDone too.
	in <- ShardWriteResult{PutID: 99, ShardIdx: 0, Err: fmt.Errorf("eio")}
	in <- ShardWriteResult{PutID: 99, ShardIdx: 1, Err: fmt.Errorf("eio")}
	in <- ShardWriteResult{PutID: 99, ShardIdx: 2, Bytes: 100}
	in <- ShardWriteResult{PutID: 99, ShardIdx: 3, Bytes: 100}

	select {
	case err := <-early:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("earlyAck never fired")
	}
	select {
	case err := <-final:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("finalDone never fired")
	}
}
