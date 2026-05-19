package encrypt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeBackend is an in-memory Backend implementation for tests.
type fakeBackend struct {
	mu      sync.Mutex
	records map[string]*fakeRecord
}

type fakeRecord struct {
	payload []byte
	gen     uint32
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{records: make(map[string]*fakeRecord)}
}

func (f *fakeBackend) set(key string, payload []byte, gen uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[key] = &fakeRecord{payload: append([]byte(nil), payload...), gen: gen}
}

func (f *fakeBackend) IterByDEKGen(ctx context.Context, gen uint32, fn func(key string, payload []byte) error) error {
	f.mu.Lock()
	var keys []string
	for k, v := range f.records {
		if v.gen == gen {
			keys = append(keys, k)
		}
	}
	f.mu.Unlock()

	for _, k := range keys {
		f.mu.Lock()
		r := f.records[k]
		payload := append([]byte(nil), r.payload...)
		f.mu.Unlock()

		if err := fn(k, payload); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBackend) AtomicSwap(ctx context.Context, key string, newPayload []byte, newGen uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[key] = &fakeRecord{payload: append([]byte(nil), newPayload...), gen: newGen}
	return nil
}

// ReadAtomic returns a consistent (payload, gen) snapshot under the mutex.
func (f *fakeBackend) ReadAtomic(key string) ([]byte, uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.records[key]
	return append([]byte(nil), r.payload...), r.gen
}

// TestRewrapScrubber_RewritesOldGenRecords verifies that after Rotate + RewrapGeneration(0),
// the stored record is now at gen 1 and the payload is still decryptable.
func TestRewrapScrubber_RewritesOldGenRecords(t *testing.T) {
	kek := make([]byte, KEKSize)
	keeper, err := NewDEKKeeper(kek)
	require.NoError(t, err)

	// Seal a record with gen 0.
	plain := []byte("hello rewrap")
	ct, gen, err := keeper.Seal(plain)
	require.NoError(t, err)
	require.Equal(t, uint32(0), gen)

	be := newFakeBackend()
	be.set("obj/key1", ct, gen)

	// Rotate: active gen becomes 1.
	require.NoError(t, keeper.Rotate())
	activeGen, _ := keeper.Active()
	require.Equal(t, uint32(1), activeGen)

	scrubber := NewRewrapScrubber(keeper, be)
	require.NoError(t, scrubber.RewrapGeneration(context.Background(), 0))

	// Record must now be at gen 1.
	newPayload, newGen := be.ReadAtomic("obj/key1")
	require.Equal(t, uint32(1), newGen, "record gen should be updated to active gen")

	// Payload must still decrypt correctly.
	got, err := keeper.Open(newPayload, newGen)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

// TestRewrapScrubber_AtomicSwap_NoCorruptMidUpdate (F#17) asserts that concurrent readers
// never observe a mismatched (payload, gen) pair — i.e. new payload with old gen or vice versa.
func TestRewrapScrubber_AtomicSwap_NoCorruptMidUpdate(t *testing.T) {
	kek := make([]byte, KEKSize)
	keeper, err := NewDEKKeeper(kek)
	require.NoError(t, err)

	plain := []byte("atomic swap test payload")
	ct, gen, err := keeper.Seal(plain)
	require.NoError(t, err)
	require.Equal(t, uint32(0), gen)

	be := newFakeBackend()
	be.set("obj/atomic", ct, gen)

	// Rotate so gen 1 is active.
	require.NoError(t, keeper.Rotate())

	scrubber := NewRewrapScrubber(keeper, be)

	const numReaders = 50
	var (
		wg      sync.WaitGroup
		corrupt int64
		mu      sync.Mutex
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Launch concurrent readers before rewrap starts.
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				payload, readGen := be.ReadAtomic("obj/atomic")
				// Try to decrypt with the gen we read atomically — must never fail.
				_, decErr := keeper.Open(payload, readGen)
				if decErr != nil {
					mu.Lock()
					corrupt++
					mu.Unlock()
					return
				}
			}
		}()
	}

	// Run rewrap while readers are active.
	rewrapErr := scrubber.RewrapGeneration(context.Background(), 0)
	require.NoError(t, rewrapErr)

	// Give readers a moment to observe post-rewrap state, then stop.
	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, corrupt, "readers observed mismatched (payload,gen) pairs — atomicity violated")
}
