package putpipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
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
		// Negative case: generic shard errors must NOT be tagged as
		// the DEK-gen-unknown sentinel (no over-tagging to 503).
		require.False(t, errors.Is(err, encrypt.ErrDEKGenUnknown))
	case <-time.After(time.Second):
		t.Fatal("earlyAck never fired")
	}
	select {
	case err := <-final:
		require.Error(t, err)
		require.False(t, errors.Is(err, encrypt.ErrDEKGenUnknown))
	case <-time.After(time.Second):
		t.Fatal("finalDone never fired")
	}
}

// TestCommitCoord_PreservesDEKGenUnknown: when data shards fail with an
// error wrapping encrypt.ErrDEKGenUnknown, the commit coordinator must
// preserve that typed signal in the early-ack error (the path the client
// actually observes) so the HTTP layer can map it to a retriable 503.
func TestCommitCoord_PreservesDEKGenUnknown(t *testing.T) {
	in := make(chan ShardWriteResult, 4)
	meta := make(chan MetadataRecord, 1)
	c := &CommitCoord{in: in, metaBatchCh: meta, waiters: make(map[uint64]*putWaiter)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	early := make(chan error, 1)
	final := make(chan error, 1)
	c.registerPut(100, &putWaiter{
		shardsTotal: 4,
		cfg:         cluster.ECConfig{DataShards: 2, ParityShards: 2},
		earlyAck:    early,
		finalDone:   final,
		metadata:    MetadataRecord{Bucket: "b", Key: "k"},
	})

	// Both data shards fail with a DEK-gen-unknown-wrapped error.
	in <- ShardWriteResult{PutID: 100, ShardIdx: 0, Err: fmt.Errorf("seal shard 0: %w", encrypt.ErrDEKGenUnknown)}
	in <- ShardWriteResult{PutID: 100, ShardIdx: 1, Err: fmt.Errorf("seal shard 1: %w", encrypt.ErrDEKGenUnknown)}
	in <- ShardWriteResult{PutID: 100, ShardIdx: 2, Bytes: 100}
	in <- ShardWriteResult{PutID: 100, ShardIdx: 3, Bytes: 100}

	select {
	case err := <-early:
		require.Error(t, err)
		require.True(t, errors.Is(err, encrypt.ErrDEKGenUnknown),
			"early-ack error must preserve ErrDEKGenUnknown sentinel")
	case <-time.After(time.Second):
		t.Fatal("earlyAck never fired")
	}
	// final-ack is the discarded path (fire-and-forget consumer); it is not
	// asserted here — the client-observable 503 signal rides the early-ack.
}

// TestCommitCoord_MixedFailureTagsDEKGen: per the mixed-failure policy,
// if ANY shard fails with ErrDEKGenUnknown (even alongside generic
// failures), the whole aggregated error is tagged 503-retriable.
func TestCommitCoord_MixedFailureTagsDEKGen(t *testing.T) {
	in := make(chan ShardWriteResult, 4)
	meta := make(chan MetadataRecord, 1)
	c := &CommitCoord{in: in, metaBatchCh: meta, waiters: make(map[uint64]*putWaiter)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	early := make(chan error, 1)
	final := make(chan error, 1)
	c.registerPut(101, &putWaiter{
		shardsTotal: 4,
		cfg:         cluster.ECConfig{DataShards: 2, ParityShards: 2},
		earlyAck:    early,
		finalDone:   final,
		metadata:    MetadataRecord{Bucket: "b", Key: "k"},
	})

	// One generic failure, one DEK-gen-unknown failure → both data
	// shards fail. The DEK-gen shard must tag the aggregated error.
	in <- ShardWriteResult{PutID: 101, ShardIdx: 0, Err: fmt.Errorf("eio")}
	in <- ShardWriteResult{PutID: 101, ShardIdx: 1, Err: fmt.Errorf("seal shard 1: %w", encrypt.ErrDEKGenUnknown)}
	in <- ShardWriteResult{PutID: 101, ShardIdx: 2, Bytes: 100}
	in <- ShardWriteResult{PutID: 101, ShardIdx: 3, Bytes: 100}

	select {
	case err := <-early:
		require.Error(t, err)
		require.True(t, errors.Is(err, encrypt.ErrDEKGenUnknown),
			"mixed-failure early-ack must be tagged with ErrDEKGenUnknown")
	case <-time.After(time.Second):
		t.Fatal("earlyAck never fired")
	}
}
