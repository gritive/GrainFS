package server

import (
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

const (
	// emitBuf is the actor channel buffer for incoming HealEvents.
	// Sized to absorb a full repair burst without blocking callers.
	emitBuf = 512
)

// receiptTrackingEmitter wraps a base Emitter and additionally:
//  1. Buffers HealEvents per CorrelationID.
//  2. On FinalizeSession: assembles a HealReceipt, signs it, and persists it
//     to the receipt.Store.
//  3. Implements SigningHealthChecker so the scrubber can skip repairs when
//     the KeyStore has no active key.
//
// All methods are safe for concurrent use. The sessions map is owned exclusively
// by the actor goroutine — no mutex required.
type receiptTrackingEmitter struct {
	base     scrubber.Emitter
	store    *receipt.Store
	keyStore *receipt.KeyStore

	emitCh     chan scrubber.HealEvent
	finalizeCh chan finalizeReq
	countCh    chan sessionsCountReq // for test introspection
	closeOnce  sync.Once
	stopCh     chan struct{}
}

// NewReceiptTrackingEmitter creates the emitter and starts the actor goroutine.
// Call Close to stop it.
func NewReceiptTrackingEmitter(base scrubber.Emitter, store *receipt.Store, ks *receipt.KeyStore) *receiptTrackingEmitter {
	e := &receiptTrackingEmitter{
		base:       base,
		store:      store,
		keyStore:   ks,
		emitCh:     make(chan scrubber.HealEvent, emitBuf),
		finalizeCh: make(chan finalizeReq, 1),
		countCh:    make(chan sessionsCountReq, 1),
		stopCh:     make(chan struct{}),
	}
	go e.run()
	return e
}

// Close stops the actor goroutine. Idempotent.
func (e *receiptTrackingEmitter) Close() {
	e.closeOnce.Do(func() { close(e.stopCh) })
}

// Emit forwards the event to the base emitter and buffers it per CorrelationID.
// Non-blocking: if the actor channel is full, the event is dropped with a warning.
func (e *receiptTrackingEmitter) Emit(ev scrubber.HealEvent) {
	e.base.Emit(ev)

	if ev.CorrelationID == "" {
		return
	}

	select {
	case e.emitCh <- ev:
	default:
		log.Warn().Str("correlation_id", ev.CorrelationID).Msg("receipt: emitCh full, event dropped")
	}
}

// SigningHealthy reports whether the KeyStore has an active signing key.
func (e *receiptTrackingEmitter) SigningHealthy() bool {
	if e.keyStore == nil {
		return false
	}
	_, ok := e.keyStore.Active()
	return ok
}

// FinalizeSession assembles a HealReceipt from the buffered events for
// correlationID, signs it, and persists it to the Store.
// Signing and persistence happen in the caller's goroutine (not the actor),
// so slow I/O does not stall event buffering.
func (e *receiptTrackingEmitter) FinalizeSession(correlationID string) {
	reply := make(chan *receiptSession, 1)
	select {
	case e.finalizeCh <- finalizeReq{correlationID: correlationID, reply: reply}:
	case <-e.stopCh:
		return
	}

	var sess *receiptSession
	select {
	case sess = <-reply:
	case <-e.stopCh:
		return
	}
	if sess == nil || len(sess.events) == 0 {
		return
	}

	persistHealReceipt(e.store, e.keyStore, correlationID, sess.events)
}
