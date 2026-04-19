package raft

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

func fbFinishRPC(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// encodeRPC serializes an RPC message (type + payload) using FlatBuffers.
func encodeRPC(rpcType string, msg any) ([]byte, error) {
	data, err := encodeRPCPayload(rpcType, msg)
	if err != nil {
		return nil, err
	}

	b := flatbuffers.NewBuilder(len(data) + 32)
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
		args := msg.(*RequestVoteArgs)
		b := flatbuffers.NewBuilder(64)
		cidOff := b.CreateString(args.CandidateID)
		pb.RequestVoteArgsStart(b)
		pb.RequestVoteArgsAddTerm(b, args.Term)
		pb.RequestVoteArgsAddCandidateId(b, cidOff)
		pb.RequestVoteArgsAddLastLogIndex(b, args.LastLogIndex)
		pb.RequestVoteArgsAddLastLogTerm(b, args.LastLogTerm)
		root := pb.RequestVoteArgsEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeRequestVoteReply:
		reply := msg.(*RequestVoteReply)
		b := flatbuffers.NewBuilder(16)
		pb.RequestVoteReplyStart(b)
		pb.RequestVoteReplyAddTerm(b, reply.Term)
		pb.RequestVoteReplyAddVoteGranted(b, reply.VoteGranted)
		root := pb.RequestVoteReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeAppendEntries:
		args := msg.(*AppendEntriesArgs)
		b := flatbuffers.NewBuilder(256)

		// Build LogEntry objects (must be built before Start)
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
		reply := msg.(*AppendEntriesReply)
		b := flatbuffers.NewBuilder(16)
		pb.AppendEntriesReplyStart(b)
		pb.AppendEntriesReplyAddTerm(b, reply.Term)
		pb.AppendEntriesReplyAddSuccess(b, reply.Success)
		root := pb.AppendEntriesReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeInstallSnapshot:
		args := msg.(*InstallSnapshotArgs)
		b := flatbuffers.NewBuilder(len(args.Data) + 64)
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
		root := pb.InstallSnapshotArgsEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeInstallSnapshotReply:
		reply := msg.(*InstallSnapshotReply)
		b := flatbuffers.NewBuilder(16)
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

// decodeRPC deserializes the outer RPCMessage envelope, returning the type and inner payload bytes.
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

func decodeRequestVoteArgs(data []byte) (*RequestVoteArgs, error) {
	a := pb.GetRootAsRequestVoteArgs(data, 0)
	return &RequestVoteArgs{
		Term:         a.Term(),
		CandidateID:  string(a.CandidateId()),
		LastLogIndex: a.LastLogIndex(),
		LastLogTerm:  a.LastLogTerm(),
	}, nil
}

func decodeRequestVoteReply(data []byte) (*RequestVoteReply, error) {
	r := pb.GetRootAsRequestVoteReply(data, 0)
	return &RequestVoteReply{
		Term:        r.Term(),
		VoteGranted: r.VoteGranted(),
	}, nil
}

func decodeAppendEntriesArgs(data []byte) (*AppendEntriesArgs, error) {
	a := pb.GetRootAsAppendEntriesArgs(data, 0)
	entries := make([]LogEntry, a.EntriesLength())
	var e pb.LogEntry
	for i := 0; i < a.EntriesLength(); i++ {
		if !a.Entries(&e, i) {
			continue
		}
		entries[i] = LogEntry{Term: e.Term(), Index: e.Index(), Command: e.CommandBytes()}
	}
	return &AppendEntriesArgs{
		Term:         a.Term(),
		LeaderID:     string(a.LeaderId()),
		PrevLogIndex: a.PrevLogIndex(),
		PrevLogTerm:  a.PrevLogTerm(),
		Entries:      entries,
		LeaderCommit: a.LeaderCommit(),
	}, nil
}

func decodeAppendEntriesReply(data []byte) (*AppendEntriesReply, error) {
	r := pb.GetRootAsAppendEntriesReply(data, 0)
	return &AppendEntriesReply{
		Term:    r.Term(),
		Success: r.Success(),
	}, nil
}

func decodeInstallSnapshotArgs(data []byte) (*InstallSnapshotArgs, error) {
	a := pb.GetRootAsInstallSnapshotArgs(data, 0)
	return &InstallSnapshotArgs{
		Term:              a.Term(),
		LeaderID:          string(a.LeaderId()),
		LastIncludedIndex: a.LastIncludedIndex(),
		LastIncludedTerm:  a.LastIncludedTerm(),
		Data:              a.DataBytes(),
	}, nil
}

func decodeInstallSnapshotReply(data []byte) (*InstallSnapshotReply, error) {
	r := pb.GetRootAsInstallSnapshotReply(data, 0)
	return &InstallSnapshotReply{
		Term: r.Term(),
	}, nil
}
