// raft_codec.go — wire codec for the cluster Raft RPC bridge.
//
// It encodes/decodes Raft RPCs as FlatBuffers RPC envelopes, sharing the schema
// package (internal/raft/raftpb) with internal/raft's own codec. This is the
// sole cluster-side raft RPC codec; the QUIC-era v1 it once mirrored is deleted,
// so it is free to evolve (greenfield/flag-day — no rolling-upgrade compat with
// a v1 peer).
//
// The current wire bytes are pinned as a regression guard by the golden-hex
// cases in raft_codec_test.go (TestV2EncodeRPC_WireGolden): the FlatBuffers
// vtable layout is deterministic given the builder call sequence, so changing
// that sequence shifts the bytes and an intentional wire change must regenerate
// the goldens.

package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

// RPC type strings — wire literals, pinned by the golden tests (historically
// matched the deleted v1 codec's constants verbatim).
const (
	rpcTypeRequestVote          = "RequestVote"
	rpcTypeRequestVoteReply     = "RequestVoteReply"
	rpcTypeAppendEntries        = "AppendEntries"
	rpcTypeAppendEntriesReply   = "AppendEntriesReply"
	rpcTypeInstallSnapshot      = "InstallSnapshot"
	rpcTypeInstallSnapshotReply = "InstallSnapshotReply"
	rpcTypeTimeoutNow           = "TimeoutNow"
	rpcTypeTimeoutNowReply      = "TimeoutNowReply"
)

// raftBuilderPool is a sibling of internal/raft's raftBuilderPool
// (package-separate pools, intentionally separate so the two packages'
// builders never share state).
var raftBuilderPool = pool.New(func() *flatbuffers.Builder { return flatbuffers.NewBuilder(256) })

func fbFinishRPC(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	raftBuilderPool.Put(b)
	return out
}

// encodeRPC serializes an RPC message (type + payload) using FlatBuffers.
// Byte-identical to internal/raft.encodeRPC.
func encodeRPC(rpcType string, msg any) ([]byte, error) {
	data, err := encodeRPCPayload(rpcType, msg)
	if err != nil {
		return nil, err
	}

	b := raftBuilderPool.Get()
	typeOff := b.CreateString(rpcType)
	var dataOff flatbuffers.UOffsetT
	if len(data) > 0 {
		dataOff = b.CreateByteVector(data)
	}
	pb.RPCMessageStart(b)
	pb.RPCMessageAddType(b, typeOff)
	if len(data) > 0 {
		pb.RPCMessageAddData(b, dataOff)
	}
	root := pb.RPCMessageEnd(b)
	return fbFinishRPC(b, root), nil
}

func encodeRPCPayload(rpcType string, msg any) ([]byte, error) {
	switch rpcType {
	case rpcTypeRequestVote:
		args := msg.(*raft.RequestVoteArgs)
		b := raftBuilderPool.Get()
		cidOff := b.CreateString(args.CandidateID)
		pb.RequestVoteArgsStart(b)
		pb.RequestVoteArgsAddTerm(b, args.Term)
		pb.RequestVoteArgsAddCandidateId(b, cidOff)
		pb.RequestVoteArgsAddLastLogIndex(b, args.LastLogIndex)
		pb.RequestVoteArgsAddLastLogTerm(b, args.LastLogTerm)
		pb.RequestVoteArgsAddPreVote(b, args.PreVote)
		pb.RequestVoteArgsAddLeaderTransfer(b, args.LeaderTransfer)
		root := pb.RequestVoteArgsEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeRequestVoteReply:
		reply := msg.(*raft.RequestVoteReply)
		b := raftBuilderPool.Get()
		pb.RequestVoteReplyStart(b)
		pb.RequestVoteReplyAddTerm(b, reply.Term)
		pb.RequestVoteReplyAddVoteGranted(b, reply.VoteGranted)
		root := pb.RequestVoteReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeAppendEntries:
		args := msg.(*raft.AppendEntriesArgs)
		b := raftBuilderPool.Get()

		entryOffs := make([]flatbuffers.UOffsetT, len(args.Entries))
		for i := len(args.Entries) - 1; i >= 0; i-- {
			e := args.Entries[i]
			var cmdOff flatbuffers.UOffsetT
			if len(e.Command) > 0 {
				cmdOff = b.CreateByteVector(e.Command)
			}
			pb.LogEntryStart(b)
			pb.LogEntryAddTerm(b, e.Term)
			pb.LogEntryAddIndex(b, e.Index)
			if len(e.Command) > 0 {
				pb.LogEntryAddCommand(b, cmdOff)
			}
			if e.Type != raft.LogEntryCommand {
				pb.LogEntryAddEntryType(b, pb.LogEntryType(e.Type))
			}
			entryOffs[i] = pb.LogEntryEnd(b)
		}

		pb.AppendEntriesArgsStartEntriesVector(b, len(entryOffs))
		for i := len(entryOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(entryOffs[i])
		}
		entriesVec := b.EndVector(len(entryOffs))

		leaderIDOff := b.CreateString(args.LeaderID)
		pb.AppendEntriesArgsStart(b)
		pb.AppendEntriesArgsAddTerm(b, args.Term)
		pb.AppendEntriesArgsAddLeaderId(b, leaderIDOff)
		pb.AppendEntriesArgsAddPrevLogIndex(b, args.PrevLogIndex)
		pb.AppendEntriesArgsAddPrevLogTerm(b, args.PrevLogTerm)
		pb.AppendEntriesArgsAddEntries(b, entriesVec)
		pb.AppendEntriesArgsAddLeaderCommit(b, args.LeaderCommit)
		root := pb.AppendEntriesArgsEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeAppendEntriesReply:
		reply := msg.(*raft.AppendEntriesReply)
		b := raftBuilderPool.Get()
		pb.AppendEntriesReplyStart(b)
		pb.AppendEntriesReplyAddTerm(b, reply.Term)
		pb.AppendEntriesReplyAddSuccess(b, reply.Success)
		pb.AppendEntriesReplyAddConflictTerm(b, reply.ConflictTerm)
		pb.AppendEntriesReplyAddConflictIndex(b, reply.ConflictIndex)
		root := pb.AppendEntriesReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeInstallSnapshot:
		args := msg.(*raft.InstallSnapshotArgs)
		b := raftBuilderPool.Get()

		serverOffs := make([]flatbuffers.UOffsetT, len(args.Servers))
		for i := len(args.Servers) - 1; i >= 0; i-- {
			s := args.Servers[i]
			idOff := b.CreateString(s.ID)
			pb.ServerEntryStart(b)
			pb.ServerEntryAddId(b, idOff)
			pb.ServerEntryAddSuffrage(b, int8(s.Suffrage))
			serverOffs[i] = pb.ServerEntryEnd(b)
		}
		var serversVec flatbuffers.UOffsetT
		if len(serverOffs) > 0 {
			pb.InstallSnapshotArgsStartServersVector(b, len(serverOffs))
			for i := len(serverOffs) - 1; i >= 0; i-- {
				b.PrependUOffsetT(serverOffs[i])
			}
			serversVec = b.EndVector(len(serverOffs))
		}

		var dataOff flatbuffers.UOffsetT
		if len(args.Data) > 0 {
			dataOff = b.CreateByteVector(args.Data)
		}
		leaderIDOff := b.CreateString(args.LeaderID)
		pb.InstallSnapshotArgsStart(b)
		pb.InstallSnapshotArgsAddTerm(b, args.Term)
		pb.InstallSnapshotArgsAddLeaderId(b, leaderIDOff)
		pb.InstallSnapshotArgsAddLastIncludedIndex(b, args.LastIncludedIndex)
		pb.InstallSnapshotArgsAddLastIncludedTerm(b, args.LastIncludedTerm)
		if len(args.Data) > 0 {
			pb.InstallSnapshotArgsAddData(b, dataOff)
		}
		if len(serverOffs) > 0 {
			pb.InstallSnapshotArgsAddServers(b, serversVec)
		}
		root := pb.InstallSnapshotArgsEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeInstallSnapshotReply:
		reply := msg.(*raft.InstallSnapshotReply)
		b := raftBuilderPool.Get()
		pb.InstallSnapshotReplyStart(b)
		pb.InstallSnapshotReplyAddTerm(b, reply.Term)
		root := pb.InstallSnapshotReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeTimeoutNow, rpcTypeTimeoutNowReply:
		return []byte{}, nil

	default:
		return nil, fmt.Errorf("unknown RPC type: %s", rpcType)
	}
}

// decodeRPC deserializes the outer RPCMessage envelope.
func decodeRPC(raw []byte) (rpcType string, data []byte, err error) {
	if len(raw) == 0 {
		return "", nil, fmt.Errorf("unmarshal RPC envelope: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal RPC envelope: invalid flatbuffer: %v", r)
		}
	}()
	msg := pb.GetRootAsRPCMessage(raw, 0)
	return string(msg.Type()), msg.DataBytes(), nil
}

func decodeRequestVoteArgs(data []byte) (args *raft.RequestVoteArgs, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode RequestVoteArgs: invalid flatbuffer: %v", r)
		}
	}()
	a := pb.GetRootAsRequestVoteArgs(data, 0)
	return &raft.RequestVoteArgs{
		Term:           a.Term(),
		CandidateID:    string(a.CandidateId()),
		LastLogIndex:   a.LastLogIndex(),
		LastLogTerm:    a.LastLogTerm(),
		PreVote:        a.PreVote(),
		LeaderTransfer: a.LeaderTransfer(),
	}, nil
}

func decodeRequestVoteReply(data []byte) (reply *raft.RequestVoteReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode RequestVoteReply: invalid flatbuffer: %v", r)
		}
	}()
	r := pb.GetRootAsRequestVoteReply(data, 0)
	return &raft.RequestVoteReply{
		Term:        r.Term(),
		VoteGranted: r.VoteGranted(),
	}, nil
}

func decodeAppendEntriesArgs(data []byte) (args *raft.AppendEntriesArgs, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode AppendEntriesArgs: invalid flatbuffer: %v", r)
		}
	}()
	a := pb.GetRootAsAppendEntriesArgs(data, 0)
	entries := make([]raft.LogEntry, a.EntriesLength())
	var e pb.LogEntry
	for i := 0; i < a.EntriesLength(); i++ {
		if !a.Entries(&e, i) {
			continue
		}
		entries[i] = raft.LogEntry{Term: e.Term(), Index: e.Index(), Command: e.CommandBytes(), Type: raft.LogEntryType(e.EntryType())}
	}
	return &raft.AppendEntriesArgs{
		Term:         a.Term(),
		LeaderID:     string(a.LeaderId()),
		PrevLogIndex: a.PrevLogIndex(),
		PrevLogTerm:  a.PrevLogTerm(),
		Entries:      entries,
		LeaderCommit: a.LeaderCommit(),
	}, nil
}

func decodeAppendEntriesReply(data []byte) (reply *raft.AppendEntriesReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode AppendEntriesReply: invalid flatbuffer: %v", r)
		}
	}()
	r := pb.GetRootAsAppendEntriesReply(data, 0)
	return &raft.AppendEntriesReply{
		Term:          r.Term(),
		Success:       r.Success(),
		ConflictTerm:  r.ConflictTerm(),
		ConflictIndex: r.ConflictIndex(),
	}, nil
}

func decodeInstallSnapshotArgs(data []byte) (args *raft.InstallSnapshotArgs, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode InstallSnapshotArgs: invalid flatbuffer: %v", r)
		}
	}()
	a := pb.GetRootAsInstallSnapshotArgs(data, 0)
	servers := make([]raft.Server, a.ServersLength())
	var se pb.ServerEntry
	for i := 0; i < a.ServersLength(); i++ {
		if a.Servers(&se, i) {
			servers[i] = raft.Server{ID: string(se.Id()), Suffrage: raft.ServerSuffrage(se.Suffrage())}
		}
	}
	return &raft.InstallSnapshotArgs{
		Term:              a.Term(),
		LeaderID:          string(a.LeaderId()),
		LastIncludedIndex: a.LastIncludedIndex(),
		LastIncludedTerm:  a.LastIncludedTerm(),
		Data:              a.DataBytes(),
		Servers:           servers,
	}, nil
}

func decodeInstallSnapshotReply(data []byte) (reply *raft.InstallSnapshotReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode InstallSnapshotReply: invalid flatbuffer: %v", r)
		}
	}()
	r := pb.GetRootAsInstallSnapshotReply(data, 0)
	return &raft.InstallSnapshotReply{
		Term: r.Term(),
	}, nil
}
