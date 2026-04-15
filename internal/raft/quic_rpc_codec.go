package raft

import (
	"fmt"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
	"google.golang.org/protobuf/proto"
)

// encodeRPC serializes an RPC message (type + payload) using protobuf.
func encodeRPC(rpcType string, msg any) ([]byte, error) {
	var data []byte
	var err error

	switch rpcType {
	case rpcTypeRequestVote:
		args := msg.(*RequestVoteArgs)
		data, err = proto.Marshal(&pb.RequestVoteArgs{
			Term:         args.Term,
			CandidateId:  args.CandidateID,
			LastLogIndex: args.LastLogIndex,
			LastLogTerm:  args.LastLogTerm,
		})
	case rpcTypeRequestVoteReply:
		reply := msg.(*RequestVoteReply)
		data, err = proto.Marshal(&pb.RequestVoteReply{
			Term:        reply.Term,
			VoteGranted: reply.VoteGranted,
		})
	case rpcTypeAppendEntries:
		args := msg.(*AppendEntriesArgs)
		entries := make([]*pb.LogEntry, len(args.Entries))
		for i, e := range args.Entries {
			entries[i] = &pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
		}
		data, err = proto.Marshal(&pb.AppendEntriesArgs{
			Term:         args.Term,
			LeaderId:     args.LeaderID,
			PrevLogIndex: args.PrevLogIndex,
			PrevLogTerm:  args.PrevLogTerm,
			Entries:      entries,
			LeaderCommit: args.LeaderCommit,
		})
	case rpcTypeAppendEntriesReply:
		reply := msg.(*AppendEntriesReply)
		data, err = proto.Marshal(&pb.AppendEntriesReply{
			Term:    reply.Term,
			Success: reply.Success,
		})
	case rpcTypeInstallSnapshot:
		args := msg.(*InstallSnapshotArgs)
		data, err = proto.Marshal(&pb.InstallSnapshotArgs{
			Term:              args.Term,
			LeaderId:          args.LeaderID,
			LastIncludedIndex: args.LastIncludedIndex,
			LastIncludedTerm:  args.LastIncludedTerm,
			Data:              args.Data,
		})
	case rpcTypeInstallSnapshotReply:
		reply := msg.(*InstallSnapshotReply)
		data, err = proto.Marshal(&pb.InstallSnapshotReply{
			Term: reply.Term,
		})
	default:
		return nil, fmt.Errorf("unknown RPC type: %s", rpcType)
	}
	if err != nil {
		return nil, fmt.Errorf("marshal %s payload: %w", rpcType, err)
	}

	envelope := &pb.RPCMessage{Type: rpcType, Data: data}
	return proto.Marshal(envelope)
}

// decodeRPC deserializes the outer RPCMessage envelope, returning the type and inner payload bytes.
func decodeRPC(data []byte) (string, []byte, error) {
	var msg pb.RPCMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		return "", nil, fmt.Errorf("unmarshal RPC envelope: %w", err)
	}
	return msg.Type, msg.Data, nil
}

func decodeRequestVoteArgs(data []byte) (*RequestVoteArgs, error) {
	var pb_ pb.RequestVoteArgs
	if err := proto.Unmarshal(data, &pb_); err != nil {
		return nil, err
	}
	return &RequestVoteArgs{
		Term:         pb_.Term,
		CandidateID:  pb_.CandidateId,
		LastLogIndex: pb_.LastLogIndex,
		LastLogTerm:  pb_.LastLogTerm,
	}, nil
}

func decodeRequestVoteReply(data []byte) (*RequestVoteReply, error) {
	var pb_ pb.RequestVoteReply
	if err := proto.Unmarshal(data, &pb_); err != nil {
		return nil, err
	}
	return &RequestVoteReply{
		Term:        pb_.Term,
		VoteGranted: pb_.VoteGranted,
	}, nil
}

func decodeAppendEntriesArgs(data []byte) (*AppendEntriesArgs, error) {
	var pb_ pb.AppendEntriesArgs
	if err := proto.Unmarshal(data, &pb_); err != nil {
		return nil, err
	}
	entries := make([]LogEntry, len(pb_.Entries))
	for i, e := range pb_.Entries {
		entries[i] = LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return &AppendEntriesArgs{
		Term:         pb_.Term,
		LeaderID:     pb_.LeaderId,
		PrevLogIndex: pb_.PrevLogIndex,
		PrevLogTerm:  pb_.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: pb_.LeaderCommit,
	}, nil
}

func decodeAppendEntriesReply(data []byte) (*AppendEntriesReply, error) {
	var pb_ pb.AppendEntriesReply
	if err := proto.Unmarshal(data, &pb_); err != nil {
		return nil, err
	}
	return &AppendEntriesReply{
		Term:    pb_.Term,
		Success: pb_.Success,
	}, nil
}

func decodeInstallSnapshotArgs(data []byte) (*InstallSnapshotArgs, error) {
	var pb_ pb.InstallSnapshotArgs
	if err := proto.Unmarshal(data, &pb_); err != nil {
		return nil, err
	}
	return &InstallSnapshotArgs{
		Term:              pb_.Term,
		LeaderID:          pb_.LeaderId,
		LastIncludedIndex: pb_.LastIncludedIndex,
		LastIncludedTerm:  pb_.LastIncludedTerm,
		Data:              pb_.Data,
	}, nil
}

func decodeInstallSnapshotReply(data []byte) (*InstallSnapshotReply, error) {
	var pb_ pb.InstallSnapshotReply
	if err := proto.Unmarshal(data, &pb_); err != nil {
		return nil, err
	}
	return &InstallSnapshotReply{
		Term: pb_.Term,
	}, nil
}
