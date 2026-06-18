package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestSetObjectTags_ConcurrentRMW_NoLostUpdate(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	_, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	const n = 20
	var wg sync.WaitGroup
	// require.NoError inside a goroutine calls t.FailNow → runtime.Goexit, which
	// only unwinds that goroutine (the failure can be missed or hang the test).
	// Capture errors and assert on the main goroutine after Wait instead.
	var mu sync.Mutex
	var errs []error
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tag := storage.Tag{Key: "k", Value: fmt.Sprintf("v%02d", i)}
			if err := b.SetObjectTags("bucket", "obj", "", []storage.Tag{tag}); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		require.NoError(t, e)
	}

	cmd, err := b.readQuorumMetaCmd("bucket", "obj")
	require.NoError(t, err)
	// Each serialized RMW bumps MetaSeq by 1 from the prior winner; n writers => MetaSeq == n.
	require.Equal(t, uint64(n), cmd.MetaSeq, "every RMW must serialize and advance MetaSeq; a lost update would leave a gap")
	require.Len(t, cmd.Tags, 1, "exactly one tag key survives")
}

func TestRelocation_HoldsMetaRMWLock(t *testing.T) {
	b := newTestDistributedBackend(t)
	lock := b.objectMetaRMWLock("bucket", "obj")
	lock.Lock() // simulate an in-progress tag RMW holding the lock
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		// relocation must block on the SAME lock before it reads cur; it will then
		// fail later (no real redundant group in this harness), but the point is it
		// must not PROCEED past the lock acquisition while we hold it.
		_ = b.relocateObjectToRedundantGroup(context.Background(), relocateInput{Bucket: "bucket", Key: "obj"})
		close(done)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("relocation proceeded without acquiring the meta-RMW lock")
	case <-time.After(100 * time.Millisecond):
		// good: blocked on the lock
	}
	lock.Unlock()
	// now it proceeds (and returns an error for the missing redundant group — fine);
	// guard against a hang so a regression deadlock fails CI instead of blocking forever.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relocation never completed after lock release")
	}
}

func TestSetObjectACL_ConcurrentRMW_NoLostUpdate(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	_, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	const n = 20
	var wg sync.WaitGroup
	// See TestSetObjectTags_ConcurrentRMW_NoLostUpdate: require inside a goroutine
	// only unwinds that goroutine, so capture errors and assert after Wait.
	var mu sync.Mutex
	var errs []error
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Alternate between ACLPrivate and ACLPublicRead so each write is meaningful.
			acl := uint8(s3auth.ACLPrivate)
			if i%2 == 0 {
				acl = uint8(s3auth.ACLPublicRead)
			}
			if err := b.SetObjectACL("bucket", "obj", acl); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		require.NoError(t, e)
	}

	cmd, err := b.readQuorumMetaCmd("bucket", "obj")
	require.NoError(t, err)
	// Each serialized RMW bumps MetaSeq by 1 from the prior winner; n writers => MetaSeq == n.
	require.Equal(t, uint64(n), cmd.MetaSeq, "every ACL RMW must serialize and advance MetaSeq; a lost update would leave a gap")
}
