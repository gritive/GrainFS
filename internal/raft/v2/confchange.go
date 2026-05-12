package raftv2

import (
	"encoding/binary"
	"fmt"
)

// ConfChange wire encoding (binary, big-endian — internal-comms rule
// prohibits JSON).
//
// Two formats coexist; the leading version byte discriminates:
//
// Joint entries (LogEntryJointConfChange) — UNCHANGED by M6.0 (§M6.0.0
// amendment #3). Joint quorum math is voter-only and learners are
// out-of-band; joint transitions never carry learner state on the wire.
//
//	[Version: 1B = 0x01]
//	[Kind:    1B = 0x01]       // joint
//	[NumNew:  4B BE][(LenN: 4B BE, BytesN) × NumNew]
//	[NumOld:  4B BE][(LenO: 4B BE, BytesO) × NumOld]
//
// Single entries (LogEntryConfChange) — Path B (M6.0) format. Carries the
// Op tag + the learner-targeted ID/Address (when applicable) + the full
// resulting voter and learner sets so a follower replaying the entry
// reconstructs both halves of effectiveConfig.
//
//	[Version: 1B = 0x02]
//	[Kind:    1B = 0x00]       // single
//	[Op:      1B]              // ConfChangeOp
//	[LearnerIDLen:   2B BE][LearnerID:   variable]
//	[LearnerAddrLen: 2B BE][LearnerAddr: variable]
//	[NumVoters:   4B BE][(LenV: 4B BE, BytesV) × NumVoters]
//	[NumLearners: 4B BE][(LenLID: 2B BE, BytesLID, LenLAddr: 2B BE, BytesLAddr) × NumLearners]
//
// The schema-version key in logstore_badger refuses to open a pre-M6.0
// single payload (version 0x01 + kind 0x00); operators must wipe and
// re-bootstrap. Joint payload version 0x01 stays valid going forward.
const (
	confChangeVersionJoint  byte = 0x01
	confChangeVersionSingle byte = 0x02
)

const (
	confChangeKindSingle byte = 0
	confChangeKindJoint  byte = 1
)

// confChangePayload is the decoded form.
//
// Joint entries (IsJoint == true): only NewVoters + OldVoters populated.
// Single entries (IsJoint == false): Op + LearnerID/LearnerAddress (when
// the op targets a learner) + NewVoters + NewLearners populated.
type confChangePayload struct {
	IsJoint        bool
	Op             ConfChangeOp // single-only; ignored when IsJoint
	LearnerID      string       // target of an AddLearner / PromoteStage1 / RemoveLearner
	LearnerAddress string       // target's transport address (AddLearner only)
	NewVoters      []string     // resulting voter set after the entry
	OldVoters      []string     // joint-only: Cold side
	NewLearners    []confLearner
}

// confLearner is a single learner entry in the resulting learners snapshot
// of a single-config ConfChange payload.
type confLearner struct {
	ID      string
	Address string
}

// encodeConfChange encodes a single-config ConfChange (Path B v2 wire) —
// the payload of LogEntryConfChange. Carries Op + the resulting voter set
// + the resulting learners set so followers replay the entry into both
// halves of effectiveConfig.
//
// learnerID/learnerAddress are the target of the op (empty for
// voter-only ops like a joint-exit AddVoter).
func encodeConfChange(op ConfChangeOp, learnerID, learnerAddress string, newVoters []string, newLearners []confLearner) []byte {
	size := 1 + 1 + 1 // version + kind + op
	size += 2 + len(learnerID)
	size += 2 + len(learnerAddress)
	size += 4 // numVoters
	for _, v := range newVoters {
		size += 4 + len(v)
	}
	size += 4 // numLearners
	for _, l := range newLearners {
		size += 2 + len(l.ID) + 2 + len(l.Address)
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = confChangeVersionSingle
	off++
	buf[off] = confChangeKindSingle
	off++
	buf[off] = byte(op)
	off++
	binary.BigEndian.PutUint16(buf[off:], uint16(len(learnerID)))
	off += 2
	copy(buf[off:], learnerID)
	off += len(learnerID)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(learnerAddress)))
	off += 2
	copy(buf[off:], learnerAddress)
	off += len(learnerAddress)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(newVoters)))
	off += 4
	for _, v := range newVoters {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	binary.BigEndian.PutUint32(buf[off:], uint32(len(newLearners)))
	off += 4
	for _, l := range newLearners {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(l.ID)))
		off += 2
		copy(buf[off:], l.ID)
		off += len(l.ID)
		binary.BigEndian.PutUint16(buf[off:], uint16(len(l.Address)))
		off += 2
		copy(buf[off:], l.Address)
		off += len(l.Address)
	}
	return buf
}

// encodeJointExitConfChange is a convenience wrapper for the final entry
// of a joint-consensus AddVoter/RemoveVoter transition. The op is
// ConfChangeAddVoter and no learner is targeted; the resulting voter set
// becomes Cnew and any live learners are carried through unchanged.
func encodeJointExitConfChange(newVoters []string, learners map[string]string) []byte {
	var ls []confLearner
	if len(learners) > 0 {
		ls = make([]confLearner, 0, len(learners))
		for id, addr := range learners {
			ls = append(ls, confLearner{ID: id, Address: addr})
		}
	}
	return encodeConfChange(ConfChangeAddVoter, "", "", newVoters, ls)
}

// encodeJointConfChange encodes a joint-config entry (Cold ∪ Cnew) — the
// payload of LogEntryJointConfChange. UNCHANGED by M6.0 (§M6.0.0
// amendment #3): learners are not carried in joint entries.
func encodeJointConfChange(oldVoters, newVoters []string) []byte {
	size := 1 + 1 + 4 // version + kind + numNew
	for _, v := range newVoters {
		size += 4 + len(v)
	}
	size += 4
	for _, v := range oldVoters {
		size += 4 + len(v)
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = confChangeVersionJoint
	off++
	buf[off] = confChangeKindJoint
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(newVoters)))
	off += 4
	for _, v := range newVoters {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	binary.BigEndian.PutUint32(buf[off:], uint32(len(oldVoters)))
	off += 4
	for _, v := range oldVoters {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	return buf
}

// decodeConfChange decodes either a single or joint payload. The version
// byte discriminates: 0x01 + kind 0x01 = joint (unchanged), 0x02 + kind
// 0x00 = single Path B. Any other (version, kind) tuple is rejected.
func decodeConfChange(val []byte) (confChangePayload, error) {
	var out confChangePayload
	if len(val) < 2 {
		return out, fmt.Errorf("raftv2: confchange: short header (%d bytes)", len(val))
	}
	ver := val[0]
	kind := val[1]
	off := 2
	switch {
	case ver == confChangeVersionJoint && kind == confChangeKindJoint:
		out.IsJoint = true
		newV, off2, err := decodeVoters(val, off, "new")
		if err != nil {
			return out, err
		}
		out.NewVoters = newV
		off = off2
		oldV, off3, err := decodeVoters(val, off, "old")
		if err != nil {
			return out, err
		}
		out.OldVoters = oldV
		off = off3
		if off != len(val) {
			return out, fmt.Errorf("raftv2: confchange: trailing %d bytes", len(val)-off)
		}
		return out, nil
	case ver == confChangeVersionSingle && kind == confChangeKindSingle:
		// Path B single-format.
		if len(val) < off+1 {
			return out, fmt.Errorf("raftv2: confchange: short op")
		}
		out.Op = ConfChangeOp(val[off])
		off++
		var err error
		out.LearnerID, off, err = readLenU16String(val, off, "learnerID")
		if err != nil {
			return out, err
		}
		out.LearnerAddress, off, err = readLenU16String(val, off, "learnerAddr")
		if err != nil {
			return out, err
		}
		newV, off2, err := decodeVoters(val, off, "voters")
		if err != nil {
			return out, err
		}
		out.NewVoters = newV
		off = off2
		if len(val) < off+4 {
			return out, fmt.Errorf("raftv2: confchange: short learners count")
		}
		n := int(binary.BigEndian.Uint32(val[off:]))
		off += 4
		if n > 0 {
			out.NewLearners = make([]confLearner, n)
			for i := 0; i < n; i++ {
				var l confLearner
				l.ID, off, err = readLenU16String(val, off, fmt.Sprintf("learner[%d].id", i))
				if err != nil {
					return out, err
				}
				l.Address, off, err = readLenU16String(val, off, fmt.Sprintf("learner[%d].addr", i))
				if err != nil {
					return out, err
				}
				out.NewLearners[i] = l
			}
		}
		if off != len(val) {
			return out, fmt.Errorf("raftv2: confchange: trailing %d bytes", len(val)-off)
		}
		return out, nil
	case ver == confChangeVersionJoint && kind == confChangeKindSingle:
		// Pre-M6.0 single payload — refuse loudly. The logstore schema
		// gate should have caught the store, but defense-in-depth here
		// keeps log replay from silently misinterpreting old voters as
		// new ones with a missing Op tag.
		return out, fmt.Errorf("raftv2: confchange: pre-M6.0 single payload (version 0x01 kind 0x00) — wipe and re-bootstrap")
	}
	return out, fmt.Errorf("raftv2: confchange: unknown (version, kind) = (0x%02x, 0x%02x)", ver, kind)
}

func readLenU16String(val []byte, off int, label string) (string, int, error) {
	if len(val) < off+2 {
		return "", off, fmt.Errorf("raftv2: confchange: short %s len", label)
	}
	n := int(binary.BigEndian.Uint16(val[off:]))
	off += 2
	if len(val) < off+n {
		return "", off, fmt.Errorf("raftv2: confchange: short %s body", label)
	}
	s := string(val[off : off+n])
	off += n
	return s, off, nil
}

func decodeVoters(val []byte, off int, label string) ([]string, int, error) {
	if len(val) < off+4 {
		return nil, off, fmt.Errorf("raftv2: confchange: short %s count", label)
	}
	n := int(binary.BigEndian.Uint32(val[off:]))
	off += 4
	var out []string
	if n > 0 {
		out = make([]string, n)
		for i := 0; i < n; i++ {
			if len(val) < off+4 {
				return nil, off, fmt.Errorf("raftv2: confchange: short %s[%d] len", label, i)
			}
			vlen := int(binary.BigEndian.Uint32(val[off:]))
			off += 4
			if len(val) < off+vlen {
				return nil, off, fmt.Errorf("raftv2: confchange: short %s[%d] body", label, i)
			}
			out[i] = string(val[off : off+vlen])
			off += vlen
		}
	}
	return out, off, nil
}

// effectiveConfig is the cluster's voter set used by the actor for quorum,
// election, replication, and ReadIndex decisions. Per Raft §4.3, a server
// uses the configuration in the most recent log entry it has appended,
// even if not yet committed.
//
// In the single state (joint == false), Voters is the cluster.
// In the joint state (joint == true), the cluster is Voters ∪ OldVoters and
// every quorum decision must be confirmed by a majority of BOTH sets
// independently.
//
// learners is an additive (Path B) sibling map of non-voting observers per
// M6.0. Learners receive replicated entries (via the leader replicating to
// every server) but their acks are NEVER counted in voter quorum math.
// Joint quorum semantics (voters / oldVoters) are byte-for-byte unchanged.
type effectiveConfig struct {
	joint     bool
	voters    []string // Cnew when joint, otherwise the only set
	oldVoters []string // Cold; only populated when joint == true

	// learners maps learner ID → its transport address. Address is
	// advisory (mirrors AddVoter's addr parameter — uninterpreted by v2
	// today, but persisted so operators can recover learner identity from
	// the log/snapshot during disaster recovery). Nil/empty map means no
	// learners. NEVER overlaps with voters or oldVoters — a server is
	// either Voter (in some side of the voter set) or Learner, never both.
	learners map[string]string
}

// newSingleConfig builds a non-joint effectiveConfig from voters. The slice
// is copied so the caller's mutations cannot perturb actor state. Learners
// default to nil; callers that need to preserve a learners map across the
// transition must set it explicitly on the returned config.
func newSingleConfig(voters []string) effectiveConfig {
	cp := make([]string, len(voters))
	copy(cp, voters)
	return effectiveConfig{joint: false, voters: cp}
}

// newJointConfig builds a joint effectiveConfig from old and new voter sets.
// Both slices are copied. Learners default to nil (callers preserve them
// via cloneLearners on the prior config).
func newJointConfig(oldV, newV []string) effectiveConfig {
	o := make([]string, len(oldV))
	copy(o, oldV)
	n := make([]string, len(newV))
	copy(n, newV)
	return effectiveConfig{joint: true, voters: n, oldVoters: o}
}

// cloneLearners returns a fresh copy of c.learners (nil if empty). Used by
// applyConfigEntry to carry learners across voter-set transitions and by
// snapshot() to detach published readState from the actor-owned map.
func (c effectiveConfig) cloneLearners() map[string]string {
	if len(c.learners) == 0 {
		return nil
	}
	out := make(map[string]string, len(c.learners))
	for k, v := range c.learners {
		out[k] = v
	}
	return out
}

// allVoters returns every voter ID the actor needs to send to (Cold ∪ Cnew
// in joint, just voters otherwise). The returned slice is fresh — callers
// may retain it. Order is Cnew-first, Cold-only-additions appended.
func (c effectiveConfig) allVoters() []string {
	if !c.joint {
		out := make([]string, len(c.voters))
		copy(out, c.voters)
		return out
	}
	seen := make(map[string]struct{}, len(c.voters)+len(c.oldVoters))
	out := make([]string, 0, len(c.voters)+len(c.oldVoters))
	for _, v := range c.voters {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, v := range c.oldVoters {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// peersExcluding returns every voter ID except self. This is the set of
// peers the leader replicates to FOR QUORUM PURPOSES (RequestVote, commit
// advance, ReadIndex round confirmation). Learners are NOT included — use
// replicasExcluding for the broader replication-send set.
func (c effectiveConfig) peersExcluding(self string) []string {
	all := c.allVoters()
	out := all[:0]
	for _, v := range all {
		if v == self {
			continue
		}
		out = append(out, v)
	}
	return out
}

// replicasExcluding returns every server the leader replicates log entries
// to: voters (Cold ∪ Cnew when joint) plus learners, minus self. Learners
// are addressed by the leader so they catch up, but their acks NEVER
// contribute to quorum math (commitOK / quorumOK / quorumOKByRound iterate
// only over voter slices).
//
// Used by send paths (broadcastHeartbeat, dispatchAppendEntries). For
// quorum-shaped work — RequestVote dispatch, peerLastRound semantics —
// callers must keep using peersExcluding.
func (c effectiveConfig) replicasExcluding(self string) []string {
	all := c.allVoters()
	out := make([]string, 0, len(all)+len(c.learners))
	for _, v := range all {
		if v == self {
			continue
		}
		out = append(out, v)
	}
	for id := range c.learners {
		if id == self {
			// Defensive: a learner is never self in practice, but the
			// guard keeps this loop coherent if invariants ever slip.
			continue
		}
		out = append(out, id)
	}
	return out
}

// isLearner reports whether id is registered as a learner in the effective
// configuration. Mutually exclusive with containsAnyVoter — used by the
// election-timer guard (a learner must never become Candidate) and by the
// catchup gate (PromoteToVoter requires id to be a learner).
func (c effectiveConfig) isLearner(id string) bool {
	if c.learners == nil {
		return false
	}
	_, ok := c.learners[id]
	return ok
}

// containsVoter reports whether id is a voter in the (Cnew side of the)
// effective config. Used to detect leader-not-in-Cnew step-down.
func (c effectiveConfig) containsVoter(id string) bool {
	for _, v := range c.voters {
		if v == id {
			return true
		}
	}
	return false
}

// containsAnyVoter reports whether id is a voter in EITHER side of the
// effective config (Cold ∪ Cnew). Per Raft §4.3, "any server from either
// configuration may serve as leader" during the joint period, so election
// eligibility must consult both sides — a Cold-only voter that has not yet
// observed the joint commit is still a legitimate voter and may be the
// only node able to drive the joint forward (e.g. shrink scenarios where
// a Cnew member is partitioned mid-transition). In the non-joint state
// this is identical to containsVoter.
func (c effectiveConfig) containsAnyVoter(id string) bool {
	if c.containsVoter(id) {
		return true
	}
	for _, v := range c.oldVoters {
		if v == id {
			return true
		}
	}
	return false
}

// quorumOK reports whether granted is a majority of the relevant voter set(s).
// In single state, granted must contain a majority of voters. In joint state,
// granted must contain a majority of voters AND a majority of oldVoters.
// granted is a set membership keyed by voter ID.
func (c effectiveConfig) quorumOK(granted map[string]bool) bool {
	if !c.joint {
		return majorityFromSet(c.voters, granted)
	}
	return majorityFromSet(c.voters, granted) && majorityFromSet(c.oldVoters, granted)
}

func majorityFromSet(set []string, granted map[string]bool) bool {
	if len(set) == 0 {
		// Empty voter set degenerates: nobody to ask. Treat as already-satisfied
		// so a degenerate config does not deadlock; this branch is reachable
		// only via misuse (callers must populate at least the leader).
		return true
	}
	count := 0
	for _, v := range set {
		if granted[v] {
			count++
		}
	}
	return count >= len(set)/2+1
}

// commitOK reports whether match[v] >= idx for a majority of voters in each
// relevant set. Self is looked up separately (selfID, selfMatch) so callers
// avoid materializing a per-call map that overlays self onto matchIndex —
// matchIndex is the leader's actor-owned map and is passed by header
// (zero alloc).
func (c effectiveConfig) commitOK(idx uint64, matchIndex map[string]uint64, selfID string, selfMatch uint64) bool {
	if !c.joint {
		return majorityMatchSet(c.voters, idx, matchIndex, selfID, selfMatch)
	}
	return majorityMatchSet(c.voters, idx, matchIndex, selfID, selfMatch) &&
		majorityMatchSet(c.oldVoters, idx, matchIndex, selfID, selfMatch)
}

func majorityMatchSet(set []string, idx uint64, matchIndex map[string]uint64, selfID string, selfMatch uint64) bool {
	if len(set) == 0 {
		return true
	}
	count := 0
	for _, v := range set {
		var m uint64
		if v == selfID {
			m = selfMatch
		} else {
			m = matchIndex[v]
		}
		if m >= idx {
			count++
		}
	}
	return count >= len(set)/2+1
}

// quorumOKByRound reports whether a majority of voters have peerLastRound
// at or above minRound. Self counts as confirmed at any round (the leader
// trivially satisfies its own round). Mirrors quorumOK's joint-aware
// semantics but consumes the leader's actor-owned peerLastRound map
// directly so the ReadIndex flush path avoids per-entry map allocation.
func (c effectiveConfig) quorumOKByRound(peerLastRound map[string]uint64, selfID string, minRound uint64) bool {
	if !c.joint {
		return majorityRoundSet(c.voters, peerLastRound, selfID, minRound)
	}
	return majorityRoundSet(c.voters, peerLastRound, selfID, minRound) &&
		majorityRoundSet(c.oldVoters, peerLastRound, selfID, minRound)
}

func majorityRoundSet(set []string, peerLastRound map[string]uint64, selfID string, minRound uint64) bool {
	if len(set) == 0 {
		return true
	}
	count := 0
	for _, v := range set {
		if v == selfID {
			count++
			continue
		}
		if peerLastRound[v] >= minRound {
			count++
		}
	}
	return count >= len(set)/2+1
}

// configEntryPayload extracts a confChangePayload from a log entry whose
// Type is LogEntryConfChange or LogEntryJointConfChange. Panics on decode
// failure — a malformed config entry in the log is a corrupt-state bug.
func configEntryPayload(e LogEntry) confChangePayload {
	p, err := decodeConfChange(e.Command)
	if err != nil {
		panic(fmt.Sprintf("raftv2: configEntryPayload: decode at index %d: %v", e.Index, err))
	}
	return p
}

// applyConfigEntry returns the effectiveConfig that results from appending
// a config entry to a log under prev. It does NOT mutate prev. Callers use
// this both at append time and at log replay time during recovery.
//
// Joint entries (LogEntryJointConfChange) carry only voter-set transitions
// — learners survive unchanged (the joint encoder is voter-only).
//
// Single entries (LogEntryConfChange) carry the full resulting voter +
// learner snapshot per the Path B wire format. Followers replay each
// single entry by adopting NewVoters wholesale (settling out of joint
// state, if any) and rebuilding learners from NewLearners.
func applyConfigEntry(prev effectiveConfig, e LogEntry) effectiveConfig {
	p := configEntryPayload(e)
	if e.Type == LogEntryJointConfChange {
		next := newJointConfig(p.OldVoters, p.NewVoters)
		next.learners = prev.cloneLearners()
		return next
	}
	// Single-phase: settle on Cnew (which equals prev.voters for an
	// AddLearner/Promote/RemoveLearner op, or Cnew for a joint-exit).
	next := newSingleConfig(p.NewVoters)
	if len(p.NewLearners) > 0 {
		next.learners = make(map[string]string, len(p.NewLearners))
		for _, l := range p.NewLearners {
			next.learners[l.ID] = l.Address
		}
	}
	return next
}

// seedConfigFromCfg builds the bootstrap effective config from cfg.ID +
// cfg.Peers (which excludes self). Used when no snapshot exists.
func seedConfigFromCfg(selfID string, peers []string) effectiveConfig {
	voters := make([]string, 0, len(peers)+1)
	voters = append(voters, selfID)
	voters = append(voters, peers...)
	return newSingleConfig(voters)
}

// reconstructConfig rebuilds the effective config (and configHistory +
// appendedConfigIndex) from durable state on startup. Walks any config-
// bearing entries in the live log forward from the snapshot boundary so a
// restarted node observes the same effective config it had before the
// crash.
//
// Caller passes the loaded snapshot (may be nil), the log store with its
// FirstIndex/LastIndex already populated, and the seed identity. Returns
// the live config + a configHistory slice + the index of the last config
// entry observed.
func reconstructConfig(snap *Snapshot, logStore LogStore, selfID string, peers []string) (effectiveConfig, []configHistoryEntry, uint64) {
	var current effectiveConfig
	if snap != nil && len(snap.Configuration) > 0 {
		current = newSingleConfig(snap.Configuration)
	} else {
		current = seedConfigFromCfg(selfID, peers)
	}

	var history []configHistoryEntry
	var appendedIdx uint64
	first := logStore.FirstIndex()
	last := logStore.LastIndex()
	for i := first; i <= last; i++ {
		e, err := logStore.Entry(i)
		if err != nil {
			panic(fmt.Sprintf("raftv2: reconstructConfig: log Entry(%d): %v", i, err))
		}
		if e.Type != LogEntryConfChange && e.Type != LogEntryJointConfChange {
			continue
		}
		history = append(history, configHistoryEntry{logIndex: i, prev: current})
		current = applyConfigEntry(current, e)
		appendedIdx = i
	}
	return current, history, appendedIdx
}
