package scrubber

import (
	"context"
	"fmt"
	"strings"
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

// directorEnv는 controller goroutine이 단독 소유하는 environment다.
// directorCmd.apply가 이 env를 mutate한다. controller 외 어떤 goroutine도
// env 필드를 직접 만지지 않는다.
type directorEnv struct {
	sources   map[string]BlockSource
	verifiers map[string]BlockVerifier
	sessions  map[string]*liveSession
	dedup     map[string]string

	queue    chan triggerReq // controller → worker dispatch
	nodeID   string
	incident IncidentRecorder
}

// directorCmd는 controller inbox 메시지의 marker interface다.
// apply는 반드시 controller goroutine에서만 호출된다 (race-free 보장).
type directorCmd interface {
	apply(env *directorEnv)
}

// triggerCmd — admin Trigger 호출 (blocking, reply expected).
type triggerCmd struct {
	req   TriggerReq
	reply chan triggerReply
}

type triggerReply struct {
	sessionID string
	created   bool
}

// applyFromFSMCmd — FSM apply 호출 (fire-and-forget, non-blocking inbox).
type applyFromFSMCmd struct {
	entry ScrubTriggerEntry
}

// lookupDedupCmd — propose 경로 short-circuit (blocking, reply).
type lookupDedupCmd struct {
	req   TriggerReq
	reply chan lookupDedupReply
}

type lookupDedupReply struct {
	entry ScrubTriggerEntry
	ok    bool
}

// sessionsCmd — admin: 전체 세션 스냅샷.
type sessionsCmd struct {
	reply chan []Session
}

// getSessionCmd — admin: 단일 세션 조회.
type getSessionCmd struct {
	id    string
	reply chan getSessionReply
}

type getSessionReply struct {
	session Session
	ok      bool
}

// cancelCmd — admin: 세션 취소.
type cancelCmd struct {
	id    string
	reply chan error
}

func (c triggerCmd) apply(env *directorEnv) {
	dk := dedupKey(c.req)
	if existing, ok := env.dedup[dk]; ok {
		c.reply <- triggerReply{sessionID: existing, created: false}
		return
	}
	sess := newLiveSession(uuid.NewString(), c.req.Bucket, c.req.KeyPrefix, c.req.Scope, c.req.DryRun, time.Now())
	env.sessions[sess.id] = sess
	env.dedup[dk] = sess.id

	srcName := routeSourceFor(c.req.Bucket, c.req.KeyPrefix)
	tr := triggerReq{sess: sess, src: env.sources[srcName], ver: env.verifiers[srcName]}
	select {
	case env.queue <- tr:
		c.reply <- triggerReply{sessionID: sess.id, created: true}
	default:
		delete(env.dedup, dk)
		delete(env.sessions, sess.id)
		c.reply <- triggerReply{sessionID: "", created: false}
	}
}

func (c applyFromFSMCmd) apply(env *directorEnv) {
	if _, exists := env.sessions[c.entry.SessionID]; exists {
		return
	}
	startedAt := time.Now()
	if c.entry.RequestedAt != 0 {
		startedAt = time.Unix(c.entry.RequestedAt, 0)
	}
	sess := newLiveSession(c.entry.SessionID, c.entry.Bucket, c.entry.KeyPrefix, c.entry.Scope, c.entry.DryRun, startedAt)
	env.sessions[sess.id] = sess
	dk := dedupKey(TriggerReq{Bucket: c.entry.Bucket, KeyPrefix: c.entry.KeyPrefix, Scope: c.entry.Scope, DryRun: c.entry.DryRun})
	if _, ok := env.dedup[dk]; !ok {
		env.dedup[dk] = sess.id
	}
	srcName := routeSourceFor(c.entry.Bucket, c.entry.KeyPrefix)
	tr := triggerReq{sess: sess, src: env.sources[srcName], ver: env.verifiers[srcName]}
	select {
	case env.queue <- tr:
	default:
		delete(env.sessions, sess.id)
		if env.dedup[dk] == sess.id {
			delete(env.dedup, dk)
		}
		log.Warn().Str("session_id", c.entry.SessionID).Msg("scrub director: queue full, dropped FSM entry")
	}
}

func (c lookupDedupCmd) apply(env *directorEnv) {
	dk := dedupKey(c.req)
	id, ok := env.dedup[dk]
	if !ok {
		c.reply <- lookupDedupReply{ok: false}
		return
	}
	sess, ok := env.sessions[id]
	if !ok {
		c.reply <- lookupDedupReply{ok: false}
		return
	}
	c.reply <- lookupDedupReply{
		entry: ScrubTriggerEntry{
			SessionID: sess.id,
			Bucket:    sess.bucket,
			KeyPrefix: sess.keyPrefix,
			Scope:     sess.scope,
			DryRun:    sess.dryRun,
		},
		ok: true,
	}
}

func (c sessionsCmd) apply(env *directorEnv) {
	out := make([]Session, 0, len(env.sessions))
	for _, s := range env.sessions {
		out = append(out, s.snapshot())
	}
	c.reply <- out
}

func (c getSessionCmd) apply(env *directorEnv) {
	s, ok := env.sessions[c.id]
	if !ok {
		c.reply <- getSessionReply{ok: false}
		return
	}
	c.reply <- getSessionReply{session: s.snapshot(), ok: true}
}

func (c cancelCmd) apply(env *directorEnv) {
	s, ok := env.sessions[c.id]
	if !ok {
		c.reply <- fmt.Errorf("session %q not found", c.id)
		return
	}
	s.status.Store("cancelled")
	c.reply <- nil
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
	src  BlockSource
	ver  BlockVerifier
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

// LookupDedup returns the in-flight ScrubTriggerEntry for a request key, or
// (zero, false) if no session matches. Used by admin handlers to short-circuit
// duplicate triggers before raft propose so we do not consume two raft entries
// for the same logical scrub.
func (d *Director) LookupDedup(req TriggerReq) (ScrubTriggerEntry, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id, ok := d.dedup[dedupKey(req)]
	if !ok {
		return ScrubTriggerEntry{}, false
	}
	sess, ok := d.sessions[id]
	if !ok {
		return ScrubTriggerEntry{}, false
	}
	return ScrubTriggerEntry{
		SessionID: sess.id,
		Bucket:    sess.bucket,
		KeyPrefix: sess.keyPrefix,
		Scope:     sess.scope,
		DryRun:    sess.dryRun,
	}, true
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
	startedAt := time.Now()
	if entry.RequestedAt != 0 {
		startedAt = time.Unix(entry.RequestedAt, 0)
	}
	sess := newLiveSession(entry.SessionID, entry.Bucket, entry.KeyPrefix, entry.Scope, entry.DryRun, startedAt)
	d.sessions[entry.SessionID] = sess
	// Populate dedup so a re-trigger of the same (bucket, prefix, scope, dryRun)
	// short-circuits via LookupDedup (admin handler avoids burning a raft entry
	// per duplicate request).
	dk := dedupKey(TriggerReq{Bucket: entry.Bucket, KeyPrefix: entry.KeyPrefix, Scope: entry.Scope, DryRun: entry.DryRun})
	if _, ok := d.dedup[dk]; !ok {
		d.dedup[dk] = entry.SessionID
	}
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
	srcName := routeSourceFor(sess.bucket, sess.keyPrefix)
	d.mu.Lock()
	src := d.sources[srcName]
	ver := d.verifiers[srcName]
	d.mu.Unlock()
	if src == nil || ver == nil {
		log.Warn().Str("session_id", sess.id).Str("source", srcName).Msg("scrub director: no source registered")
		d.markDone(sess)
		return
	}
	ch, err := src.Iter(ctx, sess.scope, sess.bucket, sess.keyPrefix)
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

// routeSourceFor는 (bucket, keyPrefix)를 source 이름으로 매핑한다. ctx-free.
// Most internal buckets are still full-object replicated, but volume data
// blocks are written through the EC data path and must be verified as shards.
func routeSourceFor(bucket, keyPrefix string) string {
	if bucket == "__grainfs_volumes" && strings.Contains(keyPrefix, "/blk_") {
		return "ec"
	}
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
