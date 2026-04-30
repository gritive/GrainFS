package raft

import (
	flatbuffers "github.com/google/flatbuffers/go"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

// jointPhase tracks the §4.3 joint consensus state machine.
//
// Sub-project 2 implements two phases:
//
//	JointNone     — single configuration (C_old or C_new); normal operation.
//	JointEntering — C_old+new committed; both quorums required for any decision.
//
// JointLeave commit transitions Entering → None on every node via the apply path.
type jointPhase int8

const (
	JointNone     jointPhase = 0
	JointEntering jointPhase = 1
)

// JointOp mirrors raftpb.JointOp for in-memory use.
type JointOp int8

const (
	JointOpEnter JointOp = 0 // C_old → C_old+new
	JointOpLeave JointOp = 1 // C_old+new → C_new
)

// ServerEntry is the in-memory mirror of raftpb.ServerEntry. Joint entries
// carry full address+suffrage on the wire because standalone Raft groups have
// no external address registry (Decision 1).
type ServerEntry struct {
	ID       string
	Address  string
	Suffrage ServerSuffrage
}

// JointConfChange is the decoded payload of a LogEntryJointConfChange entry.
type JointConfChange struct {
	Op         JointOp
	NewServers []ServerEntry
	OldServers []ServerEntry
}

// encodeJointConfChange serializes a JointConfChange to LogEntry.Command bytes.
func encodeJointConfChange(jc JointConfChange) []byte {
	b := flatbuffers.NewBuilder(256)

	serverVec := func(servers []ServerEntry, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
		offsets := make([]flatbuffers.UOffsetT, len(servers))
		for i, s := range servers {
			id := b.CreateString(s.ID)
			addr := b.CreateString(s.Address)
			pb.ServerEntryStart(b)
			pb.ServerEntryAddId(b, id)
			pb.ServerEntryAddAddress(b, addr)
			pb.ServerEntryAddSuffrage(b, int8(s.Suffrage))
			offsets[i] = pb.ServerEntryEnd(b)
		}
		startVec(b, len(servers))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offsets[i])
		}
		return b.EndVector(len(servers))
	}

	newOff := serverVec(jc.NewServers, pb.JointConfChangeEntryStartNewServersVector)
	oldOff := serverVec(jc.OldServers, pb.JointConfChangeEntryStartOldServersVector)

	pb.JointConfChangeEntryStart(b)
	pb.JointConfChangeEntryAddOp(b, pb.JointOp(jc.Op))
	pb.JointConfChangeEntryAddNewServers(b, newOff)
	pb.JointConfChangeEntryAddOldServers(b, oldOff)
	root := pb.JointConfChangeEntryEnd(b)
	pb.FinishJointConfChangeEntryBuffer(b, root)
	return b.FinishedBytes()
}

// quorumSets returns the voter sets used for quorum decisions, with self
// always included for uniform majority arithmetic.
//
// Single mode (jointPhase == JointNone): current = config.Peers + self, old = nil.
//
//	(config.Peers in this codebase lists peer IDs excluding self; we splice self
//	in here so dualMajority/hasMajorityInSet can use a single uniform formula.)
//
// Joint mode (jointPhase == JointEntering): current = jointNewVoters,
//
//	old = jointOldVoters. Joint voter sets already carry full membership lists
//	including self (or excluding it if self is being removed).
//
// Caller MUST hold n.mu.
func (n *Node) quorumSets() (current []string, old []string) {
	if n.jointPhase == JointEntering {
		return n.jointNewVoters, n.jointOldVoters
	}
	cur := make([]string, 0, len(n.config.Peers)+1)
	cur = append(cur, n.id)
	cur = append(cur, n.config.Peers...)
	return cur, nil
}

// hasMajorityInSet reports whether matched ids form a strict majority of set.
// Self counts as matched if and only if self is a voter in this configuration —
// nodes that are not part of the configuration cannot contribute to its quorum.
//
// Caller MUST hold n.mu.
func (n *Node) hasMajorityInSet(matched map[string]bool, set []string) bool {
	if len(set) == 0 {
		return false
	}
	count := 0
	for _, id := range set {
		if id == n.id || matched[id] {
			count++
		}
	}
	return count > len(set)/2
}

// dualMajority returns true if matched satisfies majority in BOTH the current
// voter set AND (when in joint mode) the old voter set.
//
// In single mode, only the current set is checked. In joint mode (JointEntering),
// quorum requires both — this is the §4.3 safety guarantee.
//
// Caller MUST hold n.mu.
func (n *Node) dualMajority(matched map[string]bool) bool {
	current, old := n.quorumSets()
	if !n.hasMajorityInSet(matched, current) {
		return false
	}
	if old != nil && !n.hasMajorityInSet(matched, old) {
		return false
	}
	return true
}

// decodeJointConfChange deserializes a JointConfChange from LogEntry.Command bytes.
func decodeJointConfChange(data []byte) JointConfChange {
	e := pb.GetRootAsJointConfChangeEntry(data, 0)
	out := JointConfChange{Op: JointOp(e.Op())}

	read := func(length int, get func(*pb.ServerEntry, int) bool) []ServerEntry {
		if length == 0 {
			return nil
		}
		entries := make([]ServerEntry, 0, length)
		for i := 0; i < length; i++ {
			var s pb.ServerEntry
			if !get(&s, i) {
				continue
			}
			entries = append(entries, ServerEntry{
				ID:       string(s.Id()),
				Address:  string(s.Address()),
				Suffrage: ServerSuffrage(s.Suffrage()),
			})
		}
		return entries
	}

	out.NewServers = read(e.NewServersLength(), e.NewServers)
	out.OldServers = read(e.OldServersLength(), e.OldServers)
	return out
}
