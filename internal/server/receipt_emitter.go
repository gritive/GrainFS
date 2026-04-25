package server

import (
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
	// emitBuf is the actor channel buffer for incoming HealEvents.
	// Sized to absorb a full repair burst without blocking callers.
	emitBuf = 512
)

// receiptSession accumulates HealEvents for one repair session.
type receiptSession struct {
	events    []scrubber.HealEvent
	createdAt time.Time
}

// finalizeReq is sent by FinalizeSession to the actor goroutine.
type finalizeReq struct {
	correlationID string
	reply         chan<- *receiptSession
}

// sessionsCountReq is used internally (tests) to query the current session count.
type sessionsCountReq struct {
	reply chan<- int
}

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

// run is the actor goroutine. It owns the sessions map exclusively.
func (e *receiptTrackingEmitter) run() {
	sessions := make(map[string]*receiptSession)
	ticker := time.NewTicker(sessionTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case ev := <-e.emitCh:
			e.processEmit(sessions, ev)
		case req := <-e.finalizeCh:
			// Drain pending emits before finalizing to preserve send order.
			e.drainEmits(sessions)
			sess := sessions[req.correlationID]
			delete(sessions, req.correlationID)
			req.reply <- sess
		case req := <-e.countCh:
			// Drain pending emits before counting to reflect all prior Emit calls.
			e.drainEmits(sessions)
			req.reply <- len(sessions)
		case now := <-ticker.C:
			for cid, sess := range sessions {
				if now.Sub(sess.createdAt) > sessionTTL {
					delete(sessions, cid)
				}
			}
		}
	}
}

func (e *receiptTrackingEmitter) processEmit(sessions map[string]*receiptSession, ev scrubber.HealEvent) {
	if ev.CorrelationID == "" {
		return
	}
	sess, ok := sessions[ev.CorrelationID]
	if !ok {
		sess = &receiptSession{createdAt: time.Now()}
		sessions[ev.CorrelationID] = sess
	}
	if len(sess.events) < maxEventsPerSession {
		sess.events = append(sess.events, ev)
	}
}

func (e *receiptTrackingEmitter) drainEmits(sessions map[string]*receiptSession) {
drain:
	for {
		select {
		case ev := <-e.emitCh:
			e.processEmit(sessions, ev)
		default:
			break drain
		}
	}
}

// Close stops the actor goroutine.
func (e *receiptTrackingEmitter) Close() {
	close(e.stopCh)
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

	sess := <-reply
	if sess == nil || len(sess.events) == 0 {
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

// sessionCount returns the number of open sessions (test helper).
func (e *receiptTrackingEmitter) sessionCount() int {
	reply := make(chan int, 1)
	select {
	case e.countCh <- sessionsCountReq{reply: reply}:
		return <-reply
	case <-e.stopCh:
		return 0
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
