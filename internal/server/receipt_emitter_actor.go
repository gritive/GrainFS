package server

import (
	"time"

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

// finalizeReq is sent by FinalizeSession to the actor goroutine.
type finalizeReq struct {
	correlationID string
	reply         chan<- *receiptSession
}

// sessionsCountReq is used internally (tests) to query the current session count.
type sessionsCountReq struct {
	reply chan<- int
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

// sessionCount returns the number of open sessions.
//
//nolint:unused // package tests inspect actor state through this method.
func (e *receiptTrackingEmitter) sessionCount() int {
	reply := make(chan int, 1)
	select {
	case e.countCh <- sessionsCountReq{reply: reply}:
		return <-reply
	case <-e.stopCh:
		return 0
	}
}
