package raftv2

import (
	"encoding/binary"
	"fmt"
)

// confChangeEncodingVersion is the leading byte of every ConfChange /
// JointConfChange payload. Bumping the version lets future encodings reject
// blobs they cannot parse rather than silently misinterpret them.
//
// Wire format (binary, big-endian — internal-comms rule prohibits JSON):
//
//	[Version: 1B = 0x01]
//	[Kind:    1B]              // 0 = single config, 1 = joint config
//	[NumNew:  4B BE]
//	  Repeated NumNew times:
//	    [LenN: 4B BE][BytesN: LenN bytes]
//	If Kind == joint:
//	  [NumOld: 4B BE]
//	    Repeated NumOld times:
//	      [LenO: 4B BE][BytesO: LenO bytes]
//
// "New" means Cnew (the target configuration). For a single config entry,
// only Cnew is encoded. For a joint entry, both Cold (the configuration
// effective immediately before the change) and Cnew (the configuration we
// transition to once joint commits) are encoded.
const confChangeEncodingVersion byte = 0x01

const (
	confChangeKindSingle byte = 0
	confChangeKindJoint  byte = 1
)

// confChangePayload is the decoded form. NewVoters is always populated;
// OldVoters is populated only when IsJoint == true.
type confChangePayload struct {
	IsJoint   bool
	NewVoters []string
	OldVoters []string
}

// encodeConfChange encodes a single-config ConfChange (Cnew only) — the
// payload of LogEntryConfChange.
func encodeConfChange(newVoters []string) []byte {
	return encodePayload(false, newVoters, nil)
}

// encodeJointConfChange encodes a joint-config entry (Cold ∪ Cnew) — the
// payload of LogEntryJointConfChange.
func encodeJointConfChange(oldVoters, newVoters []string) []byte {
	return encodePayload(true, newVoters, oldVoters)
}

func encodePayload(joint bool, newVoters, oldVoters []string) []byte {
	size := 1 + 1 + 4 // version + kind + numNew
	for _, v := range newVoters {
		size += 4 + len(v)
	}
	if joint {
		size += 4
		for _, v := range oldVoters {
			size += 4 + len(v)
		}
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = confChangeEncodingVersion
	off++
	if joint {
		buf[off] = confChangeKindJoint
	} else {
		buf[off] = confChangeKindSingle
	}
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(newVoters)))
	off += 4
	for _, v := range newVoters {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	if joint {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(oldVoters)))
		off += 4
		for _, v := range oldVoters {
			binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
			off += 4
			copy(buf[off:], v)
			off += len(v)
		}
	}
	return buf
}

// decodeConfChange decodes either a single or joint payload. Caller is
// expected to dispatch on the LogEntryType, but the payload itself is
// self-describing via the Kind byte.
func decodeConfChange(val []byte) (confChangePayload, error) {
	var out confChangePayload
	if len(val) < 1 {
		return out, fmt.Errorf("raftv2: confchange: empty payload")
	}
	if val[0] != confChangeEncodingVersion {
		return out, fmt.Errorf("raftv2: confchange: unknown version 0x%02x", val[0])
	}
	off := 1
	if len(val) < off+1 {
		return out, fmt.Errorf("raftv2: confchange: short kind")
	}
	kind := val[off]
	off++
	switch kind {
	case confChangeKindSingle:
		out.IsJoint = false
	case confChangeKindJoint:
		out.IsJoint = true
	default:
		return out, fmt.Errorf("raftv2: confchange: unknown kind 0x%02x", kind)
	}

	newV, off2, err := decodeVoters(val, off, "new")
	if err != nil {
		return out, err
	}
	out.NewVoters = newV
	off = off2

	if out.IsJoint {
		oldV, off3, err := decodeVoters(val, off, "old")
		if err != nil {
			return out, err
		}
		out.OldVoters = oldV
		off = off3
	}
	if off != len(val) {
		return out, fmt.Errorf("raftv2: confchange: trailing %d bytes", len(val)-off)
	}
	return out, nil
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
type effectiveConfig struct {
	joint     bool
	voters    []string // Cnew when joint, otherwise the only set
	oldVoters []string // Cold; only populated when joint == true
}

// newSingleConfig builds a non-joint effectiveConfig from voters. The slice
// is copied so the caller's mutations cannot perturb actor state.
func newSingleConfig(voters []string) effectiveConfig {
	cp := make([]string, len(voters))
	copy(cp, voters)
	return effectiveConfig{joint: false, voters: cp}
}

// newJointConfig builds a joint effectiveConfig from old and new voter sets.
// Both slices are copied.
func newJointConfig(oldV, newV []string) effectiveConfig {
	o := make([]string, len(oldV))
	copy(o, oldV)
	n := make([]string, len(newV))
	copy(n, newV)
	return effectiveConfig{joint: true, voters: n, oldVoters: o}
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
// peers the leader replicates to.
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
func applyConfigEntry(prev effectiveConfig, e LogEntry) effectiveConfig {
	p := configEntryPayload(e)
	if e.Type == LogEntryJointConfChange {
		// Joint entry: encoded as old + new. Move into joint state.
		return newJointConfig(p.OldVoters, p.NewVoters)
	}
	// LogEntryConfChange: leave joint, settle on Cnew.
	_ = prev
	return newSingleConfig(p.NewVoters)
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
