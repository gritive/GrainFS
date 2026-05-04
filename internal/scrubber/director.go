package scrubber

import (
	"context"
	"fmt"
	"sync"
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
	sessions  map[string]*Session
	dedup     map[string]string
	queue     chan triggerReq
	incident  IncidentRecorder
	stop      chan struct{}
	nodeID    string
}

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
	sess *Session
}

func NewDirector(opts DirectorOpts) *Director {
	if opts.QueueSize == 0 {
		opts.QueueSize = 64
	}
	return &Director{
		sources:   map[string]BlockSource{},
		verifiers: map[string]BlockVerifier{},
		sessions:  map[string]*Session{},
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
	sess := &Session{
		ID:        uuid.NewString(),
		Bucket:    req.Bucket,
		KeyPrefix: req.KeyPrefix,
		Scope:     req.Scope,
		DryRun:    req.DryRun,
		StartedAt: time.Now(),
		Status:    "running",
	}
	d.sessions[sess.ID] = sess
	d.dedup[dk] = sess.ID
	select {
	case d.queue <- triggerReq{sess: sess}:
	default:
		delete(d.dedup, dk)
		delete(d.sessions, sess.ID)
		return "", false
	}
	return sess.ID, true
}

func (d *Director) ApplyFromFSM(entry ScrubTriggerEntry) {
	d.mu.Lock()
	if _, exists := d.sessions[entry.SessionID]; exists {
		d.mu.Unlock()
		return
	}
	sess := &Session{
		ID:        entry.SessionID,
		Bucket:    entry.Bucket,
		KeyPrefix: entry.KeyPrefix,
		Scope:     entry.Scope,
		DryRun:    entry.DryRun,
		StartedAt: time.Unix(entry.RequestedAt, 0),
		Status:    "running",
	}
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

func (d *Director) runSession(ctx context.Context, sess *Session) {
	srcName := d.routeSource(sess.Bucket)
	d.mu.Lock()
	src := d.sources[srcName]
	ver := d.verifiers[srcName]
	d.mu.Unlock()
	if src == nil || ver == nil {
		log.Warn().Str("session_id", sess.ID).Str("source", srcName).Msg("scrub director: no source registered")
		d.markDone(sess)
		return
	}
	ch, err := src.Iter(ctx, sess.Scope, sess.KeyPrefix)
	if err != nil {
		log.Warn().Err(err).Msg("scrub director: source iter failed")
		d.markDone(sess)
		return
	}
	for blk := range ch {
		select {
		case <-ctx.Done():
			for range ch {
			}
			return
		default:
		}
		sess.Stats.Checked++
		st, vErr := ver.Verify(ctx, blk)
		if vErr != nil {
			log.Warn().Err(vErr).Str("key", blk.Key).Msg("scrub: verify failed")
			continue
		}
		if st.Healthy {
			sess.Stats.Healthy++
			continue
		}
		sess.Stats.Detected++
		corrID := blk.Bucket + "/" + blk.Key + "/" + sess.ID
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
		if sess.DryRun {
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
			sess.Stats.Unrepairable++
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
		sess.Stats.Repaired++
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

func (d *Director) markDone(sess *Session) {
	d.mu.Lock()
	sess.DoneAt = time.Now()
	if sess.Status == "running" {
		sess.Status = "done"
	}
	d.mu.Unlock()
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
	defer d.mu.Unlock()
	out := make([]Session, 0, len(d.sessions))
	for _, s := range d.sessions {
		out = append(out, *s)
	}
	return out
}

func (d *Director) GetSession(id string) (Session, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.sessions[id]
	if !ok {
		return Session{}, false
	}
	return *s, true
}

func (d *Director) CancelSession(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.sessions[id]
	if !ok {
		return fmt.Errorf("session %q not found", id)
	}
	s.Status = "cancelled"
	return nil
}

func dedupKey(r TriggerReq) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%t", r.Bucket, r.KeyPrefix, r.Scope, r.DryRun)
}
