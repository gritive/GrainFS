package server

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

const (
	// maxEventsPerSession caps buffered events per correlationID to prevent
	// unbounded memory growth when FinalizeSession is never called.
	maxEventsPerSession = 256
	// sessionTTL is how long an unfinalized session lingers before being
	// evicted by the background sweeper (orphan protection).
	sessionTTL = 5 * time.Minute
)

// receiptSession accumulates HealEvents for one repair session.
type receiptSession struct {
	events    []scrubber.HealEvent
	createdAt time.Time
}

// receiptTrackingEmitter wraps a base Emitter and additionally:
//  1. Buffers HealEvents per CorrelationID.
//  2. On FinalizeSession: assembles a HealReceipt, signs it, and persists it
//     to the receipt.Store.
//  3. Implements SigningHealthChecker so the scrubber can skip repairs when
//     the KeyStore has no active key.
//
// All methods are safe for concurrent use.
type receiptTrackingEmitter struct {
	base     scrubber.Emitter
	store    *receipt.Store
	keyStore *receipt.KeyStore

	mu       sync.Mutex
	sessions map[string]*receiptSession

	stopCh chan struct{}
}

// NewReceiptTrackingEmitter creates the emitter and starts the orphan-sweeper
// goroutine. Call Close (or cancel the parent context) to stop it.
func NewReceiptTrackingEmitter(base scrubber.Emitter, store *receipt.Store, ks *receipt.KeyStore) *receiptTrackingEmitter {
	e := &receiptTrackingEmitter{
		base:     base,
		store:    store,
		keyStore: ks,
		sessions: make(map[string]*receiptSession),
		stopCh:   make(chan struct{}),
	}
	go e.sweepOrphans()
	return e
}

// Close stops background goroutines.
func (e *receiptTrackingEmitter) Close() {
	close(e.stopCh)
}

// Emit forwards the event to the base emitter and buffers it per CorrelationID.
func (e *receiptTrackingEmitter) Emit(ev scrubber.HealEvent) {
	e.base.Emit(ev)

	if ev.CorrelationID == "" {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	sess, ok := e.sessions[ev.CorrelationID]
	if !ok {
		sess = &receiptSession{createdAt: time.Now()}
		e.sessions[ev.CorrelationID] = sess
	}
	if len(sess.events) < maxEventsPerSession {
		sess.events = append(sess.events, ev)
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
func (e *receiptTrackingEmitter) FinalizeSession(correlationID string) {
	e.mu.Lock()
	sess, ok := e.sessions[correlationID]
	if ok {
		delete(e.sessions, correlationID)
	}
	e.mu.Unlock()

	if !ok || len(sess.events) == 0 {
		return
	}

	r := buildReceipt(correlationID, sess.events)
	if err := receipt.Sign(r, e.keyStore); err != nil {
		log.Warn().Str("correlation_id", correlationID).Err(err).Msg("receipt: sign failed, session not persisted")
		return
	}
	if err := e.store.Put(r); err != nil {
		log.Warn().Str("correlation_id", correlationID).Str("receipt_id", r.ReceiptID).Err(err).Msg("receipt: store failed")
	}
}

// sweepOrphans removes sessions that were never finalized (e.g. scrubber
// crashed mid-cycle) to prevent unbounded memory growth.
func (e *receiptTrackingEmitter) sweepOrphans() {
	ticker := time.NewTicker(sessionTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			e.mu.Lock()
			for cid, sess := range e.sessions {
				if now.Sub(sess.createdAt) > sessionTTL {
					delete(e.sessions, cid)
				}
			}
			e.mu.Unlock()
		}
	}
}

// buildReceipt assembles a HealReceipt from the session's events.
func buildReceipt(correlationID string, events []scrubber.HealEvent) *receipt.HealReceipt {
	if len(events) == 0 {
		return nil
	}

	first := events[0]
	var (
		shardsLost    []int32
		shardsRebuilt []int32
		eventIDs      []string
		peers         []string
		peerSet       = make(map[string]struct{})
		startTime     = first.Timestamp
		endTime       = first.Timestamp
		bytesRepaired int64
	)

	for _, ev := range events {
		eventIDs = append(eventIDs, ev.ID)
		if ev.Timestamp.Before(startTime) {
			startTime = ev.Timestamp
		}
		if ev.Timestamp.After(endTime) {
			endTime = ev.Timestamp
		}
		if ev.PeerID != "" {
			if _, seen := peerSet[ev.PeerID]; !seen {
				peerSet[ev.PeerID] = struct{}{}
				peers = append(peers, ev.PeerID)
			}
		}
		bytesRepaired += ev.BytesRepaired
		switch ev.Phase {
		case scrubber.PhaseDetect:
			if ev.ShardID >= 0 {
				shardsLost = append(shardsLost, ev.ShardID)
			}
		case scrubber.PhaseWrite:
			if ev.Outcome == scrubber.OutcomeSuccess && ev.ShardID >= 0 {
				shardsRebuilt = append(shardsRebuilt, ev.ShardID)
			}
		}
	}

	durationMs := uint32(endTime.Sub(startTime).Milliseconds())

	return &receipt.HealReceipt{
		ReceiptID:     uuid.Must(uuid.NewV7()).String(),
		Timestamp:     startTime,
		Object:        receipt.ObjectRef{Bucket: first.Bucket, Key: first.Key, VersionID: first.VersionID},
		ShardsLost:    shardsLost,
		ShardsRebuilt: shardsRebuilt,
		PeersInvolved: peers,
		DurationMs:    durationMs,
		EventIDs:      eventIDs,
		CorrelationID: correlationID,
	}
}
