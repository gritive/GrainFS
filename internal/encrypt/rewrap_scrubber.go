package encrypt

import "context"

// Backend is the storage-side surface the scrubber needs. The real
// implementation lives in internal/storage and is wired in Task 14;
// tests use an in-memory fake.
type Backend interface {
	// IterByDEKGen calls fn for every stored record whose dek_gen equals gen.
	// Implementations must deliver a consistent snapshot of (key, payload) per call to fn.
	IterByDEKGen(ctx context.Context, gen uint32, fn func(key string, payload []byte) error) error

	// AtomicSwap atomically replaces the payload and dek_gen for key.
	// The update of payload and gen MUST be visible to readers as a single unit (F#17).
	AtomicSwap(ctx context.Context, key string, newPayload []byte, newGen uint32) error
}

// RewrapScrubber walks records encrypted under an old DEK generation, decrypts
// them with the old DEK, reseals with the current active DEK, and atomically
// updates storage via AtomicSwap.
type RewrapScrubber struct {
	k  *DEKKeeper
	be Backend
}

// NewRewrapScrubber creates a RewrapScrubber backed by the given DEKKeeper and Backend.
func NewRewrapScrubber(k *DEKKeeper, be Backend) *RewrapScrubber {
	return &RewrapScrubber{k: k, be: be}
}

// RewrapGeneration walks every record at oldGen, decrypts with the old DEK,
// reseals with the active DEK, and atomic-swaps payload+gen on the backend.
// The function is safe to call while other goroutines read records; atomicity
// of the (payload, gen) update is enforced by the Backend implementation.
//
// Uses DEKKeeper.Rewrap which folds Open+Seal into a single RLock acquisition,
// halving the per-record locking overhead.
func (s *RewrapScrubber) RewrapGeneration(ctx context.Context, oldGen uint32) error {
	return s.be.IterByDEKGen(ctx, oldGen, func(key string, payload []byte) error {
		newCT, newGen, err := s.k.Rewrap(payload, oldGen)
		if err != nil {
			return err
		}
		return s.be.AtomicSwap(ctx, key, newCT, newGen)
	})
}
