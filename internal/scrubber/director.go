package scrubber

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/storage"
)

// IncidentRecorder is the slim interface Director uses to emit scrub events.
// Matches *incident.Recorder.Record(ctx, []incident.Fact) error.
type IncidentRecorder interface {
	Record(ctx context.Context, facts []incident.Fact) error
}

type Director struct {
	mu        sync.Mutex
	sources   map[string]BlockSource
	verifiers map[string]BlockVerifier
	sessions  map[string]*liveSession
	dedup     map[string]string
	queue     chan triggerReq
	incident  IncidentRecorder
	stop      chan struct{}
	nodeID    string
}

// Session is the public snapshot of a scrub session as returned by
// Director.Sessions / GetSession. Counters are plain int64 because the
// snapshot is taken under d.mu — writers use the atomic-backed liveSession.
type Session struct {
	ID        string
	Bucket    string
	KeyPrefix string
	Scope     ScrubScope
	DryRun    bool
	StartedAt time.Time
	DoneAt    time.Time
	Stats     SessionStats
	Status    string // "running" | "done" | "cancelled"
}

type SessionStats struct {
	Checked      int64
	Healthy      int64
	Detected     int64
	Repaired     int64
	Unrepairable int64
	Skipped      int64
}

// liveSession is the in-flight session state. Counter mutations happen on
// the atomic fields without holding d.mu; readers (Sessions, GetSession)
// snapshot via load() into a public Session copy.
type liveSession struct {
	id        string
	bucket    string
	keyPrefix string
	scope     ScrubScope
	dryRun    bool
	startedAt time.Time
	doneAt    atomic.Int64 // unix nanos; 0 = not done
	status    atomic.Value // string: "running" | "done" | "cancelled"

	checked      atomic.Int64
	healthy      atomic.Int64
	detected     atomic.Int64
	repaired     atomic.Int64
	unrepairable atomic.Int64
	skipped      atomic.Int64
}

func (s *liveSession) snapshot() Session {
	out := Session{
		ID:        s.id,
		Bucket:    s.bucket,
		KeyPrefix: s.keyPrefix,
		Scope:     s.scope,
		DryRun:    s.dryRun,
		StartedAt: s.startedAt,
		Stats: SessionStats{
			Checked:      s.checked.Load(),
			Healthy:      s.healthy.Load(),
			Detected:     s.detected.Load(),
			Repaired:     s.repaired.Load(),
			Unrepairable: s.unrepairable.Load(),
			Skipped:      s.skipped.Load(),
		},
	}
	if v := s.status.Load(); v != nil {
		out.Status = v.(string)
	}
	if t := s.doneAt.Load(); t != 0 {
		out.DoneAt = time.Unix(0, t)
	}
	return out
}

type TriggerReq struct {
	Bucket    string
	KeyPrefix string
	Scope     ScrubScope
	DryRun    bool
}

type ScrubTriggerEntry struct {
	SessionID        string
	Bucket           string
	KeyPrefix        string
	Scope            ScrubScope
	DryRun           bool
	RequestedAt      int64
	OriginatorNodeID string
}

type DirectorOpts struct {
	Incident  IncidentRecorder
	QueueSize int
	NodeID    string
}

type triggerReq struct {
	sess *liveSession
}

func NewDirector(opts DirectorOpts) *Director {
	if opts.QueueSize == 0 {
		opts.QueueSize = 64
	}
	return &Director{
		sources:   map[string]BlockSource{},
		verifiers: map[string]BlockVerifier{},
		sessions:  map[string]*liveSession{},
		dedup:     map[string]string{},
		queue:     make(chan triggerReq, opts.QueueSize),
		incident:  opts.Incident,
		stop:      make(chan struct{}),
		nodeID:    opts.NodeID,
	}
}

func (d *Director) Register(name string, src BlockSource, ver BlockVerifier) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sources[name] = src
	d.verifiers[name] = ver
}

func (d *Director) Trigger(req TriggerReq) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dk := dedupKey(req)
	if existing, ok := d.dedup[dk]; ok {
		return existing, false
	}
	sess := newLiveSession(uuid.NewString(), req.Bucket, req.KeyPrefix, req.Scope, req.DryRun, time.Now())
	d.sessions[sess.id] = sess
	d.dedup[dk] = sess.id
	select {
	case d.queue <- triggerReq{sess: sess}:
	default:
		delete(d.dedup, dk)
		delete(d.sessions, sess.id)
		return "", false
	}
	return sess.id, true
}

func newLiveSession(id, bucket, keyPrefix string, scope ScrubScope, dryRun bool, startedAt time.Time) *liveSession {
	s := &liveSession{
		id:        id,
		bucket:    bucket,
		keyPrefix: keyPrefix,
		scope:     scope,
		dryRun:    dryRun,
		startedAt: startedAt,
	}
	s.status.Store("running")
	return s
}

func (d *Director) ApplyFromFSM(entry ScrubTriggerEntry) {
	d.mu.Lock()
	if _, exists := d.sessions[entry.SessionID]; exists {
		d.mu.Unlock()
		return
	}
	sess := newLiveSession(entry.SessionID, entry.Bucket, entry.KeyPrefix, entry.Scope, entry.DryRun, time.Unix(entry.RequestedAt, 0))
	d.sessions[entry.SessionID] = sess
	d.mu.Unlock()
	select {
	case d.queue <- triggerReq{sess: sess}:
	default:
		log.Warn().Str("session_id", entry.SessionID).Msg("scrub director: queue full, dropped FSM entry")
	}
}

func (d *Director) Start(ctx context.Context) {
	go d.workerLoop(ctx)
}

func (d *Director) Stop() { close(d.stop) }

func (d *Director) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stop:
			return
		case req := <-d.queue:
			d.runSession(ctx, req.sess)
		}
	}
}

func (d *Director) runSession(ctx context.Context, sess *liveSession) {
	srcName := d.routeSource(sess.bucket)
	d.mu.Lock()
	src := d.sources[srcName]
	ver := d.verifiers[srcName]
	d.mu.Unlock()
	if src == nil || ver == nil {
		log.Warn().Str("session_id", sess.id).Str("source", srcName).Msg("scrub director: no source registered")
		d.markDone(sess)
		return
	}
	ch, err := src.Iter(ctx, sess.scope, sess.keyPrefix)
	if err != nil {
		log.Warn().Err(err).Msg("scrub director: source iter failed")
		d.markDone(sess)
		return
	}
	for blk := range ch {
		// Cancellation: ctx OR explicit CancelSession.
		select {
		case <-ctx.Done():
			for range ch {
			}
			return
		default:
		}
		if v := sess.status.Load(); v != nil && v.(string) == "cancelled" {
			for range ch {
			}
			d.markDone(sess) // marks DoneAt; preserves "cancelled" status
			return
		}
		sess.checked.Add(1)
		st, vErr := ver.Verify(ctx, blk)
		if vErr != nil {
			log.Warn().Err(vErr).Str("key", blk.Key).Msg("scrub: verify failed")
			continue
		}
		if st.Healthy {
			sess.healthy.Add(1)
			continue
		}
		if st.Skipped {
			sess.skipped.Add(1)
			continue
		}
		sess.detected.Add(1)
		corrID := blk.Bucket + "/" + blk.Key + "/" + sess.id
		scope := incident.Scope{
			Kind:   incident.ScopeObject,
			Bucket: blk.Bucket,
			Key:    blk.Key,
			NodeID: d.nodeID,
		}
		now := time.Now()
		facts := []incident.Fact{{
			CorrelationID: corrID,
			Type:          incident.FactObserved,
			Cause:         incident.CauseCorruptBlob,
			Scope:         scope,
			Message:       st.Detail,
			At:            now,
		}}
		if sess.dryRun {
			if d.incident != nil {
				_ = d.incident.Record(ctx, facts)
			}
			continue
		}
		facts = append(facts, incident.Fact{
			CorrelationID: corrID,
			Type:          incident.FactActionStarted,
			Action:        incident.ActionReconstructShard,
			Scope:         scope,
			At:            now,
		})
		if rerr := ver.Repair(ctx, blk); rerr != nil {
			sess.unrepairable.Add(1)
			facts = append(facts, incident.Fact{
				CorrelationID: corrID,
				Type:          incident.FactActionFailed,
				ErrorCode:     "repair_failed",
				Message:       rerr.Error(),
				Scope:         scope,
				At:            time.Now(),
			})
			if d.incident != nil {
				_ = d.incident.Record(ctx, facts)
			}
			continue
		}
		sess.repaired.Add(1)
		facts = append(facts, incident.Fact{
			CorrelationID: corrID,
			Type:          incident.FactVerified,
			Scope:         scope,
			At:            time.Now(),
		}, incident.Fact{
			CorrelationID: corrID,
			Type:          incident.FactResolved,
			Scope:         scope,
			At:            time.Now(),
		})
		if d.incident != nil {
			_ = d.incident.Record(ctx, facts)
		}
	}
	d.markDone(sess)
}

func (d *Director) markDone(sess *liveSession) {
	sess.doneAt.Store(time.Now().UnixNano())
	// Only flip "running" → "done"; leave "cancelled" intact.
	if v := sess.status.Load(); v == nil || v.(string) == "running" {
		sess.status.CompareAndSwap(v, "done")
	}
}

// routeSource maps a bucket to a registered source name. Replication-stored
// internal buckets route to "replication"; everything else falls through to
// the EC scrub source. Per-object routing (placement-record-aware) is a
// follow-up — see TODOS.md for the EC/replication mixed-bucket case.
func (d *Director) routeSource(bucket string) string {
	if storage.IsInternalBucket(bucket) {
		return "replication"
	}
	return "ec"
}

func (d *Director) Sessions() []Session {
	d.mu.Lock()
	live := make([]*liveSession, 0, len(d.sessions))
	for _, s := range d.sessions {
		live = append(live, s)
	}
	d.mu.Unlock()
	out := make([]Session, 0, len(live))
	for _, s := range live {
		out = append(out, s.snapshot())
	}
	return out
}

func (d *Director) GetSession(id string) (Session, bool) {
	d.mu.Lock()
	s, ok := d.sessions[id]
	d.mu.Unlock()
	if !ok {
		return Session{}, false
	}
	return s.snapshot(), true
}

// CancelSession marks a session cancelled. The running worker observes the
// flag at the next block boundary in runSession and stops emitting work.
// Already-issued Verify/Repair calls run to completion.
func (d *Director) CancelSession(id string) error {
	d.mu.Lock()
	s, ok := d.sessions[id]
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("session %q not found", id)
	}
	s.status.Store("cancelled")
	return nil
}

func dedupKey(r TriggerReq) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%t", r.Bucket, r.KeyPrefix, r.Scope, r.DryRun)
}
