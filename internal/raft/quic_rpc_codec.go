package raft

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/pool"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

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

type borrowedRPCPayload struct {
	data []byte
	b    *flatbuffers.Builder
}

func (p borrowedRPCPayload) release() {
	if p.b == nil {
		return
	}
	p.b.Reset()
	raftBuilderPool.Put(p.b)
}

func releaseBorrowedRPCPayloads(payloads []borrowedRPCPayload) {
	for i := range payloads {
		payloads[i].release()
		payloads[i] = borrowedRPCPayload{}
	}
}

func borrowAppendEntriesArgsPayload(args *AppendEntriesArgs) borrowedRPCPayload {
	b := raftBuilderPool.Get()
	root := buildAppendEntriesArgsFlatBuffer(b, args)
	b.Finish(root)

	return borrowedRPCPayload{data: b.FinishedBytes(), b: b}
}

func buildAppendEntriesArgsFlatBuffer(b *flatbuffers.Builder, args *AppendEntriesArgs) flatbuffers.UOffsetT {
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
		if e.Type != LogEntryCommand {
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
	return pb.AppendEntriesArgsEnd(b)
}

// encodeAppendEntriesReply serializes just the AppendEntriesReply FlatBuffer
// (no RPCMessage envelope) for batch reply payloads.
func encodeAppendEntriesReply(reply *AppendEntriesReply) ([]byte, error) {
	return encodeRPCPayload(rpcTypeAppendEntriesReply, reply)
}

// encodeRPC serializes an RPC message (type + payload) using FlatBuffers.
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
	case rpcTypeRequestVote, metaRPCRequestVote:
		args := msg.(*RequestVoteArgs)
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

	case rpcTypeRequestVoteReply, metaRPCRequestVoteReply:
		reply := msg.(*RequestVoteReply)
		b := raftBuilderPool.Get()
		pb.RequestVoteReplyStart(b)
		pb.RequestVoteReplyAddTerm(b, reply.Term)
		pb.RequestVoteReplyAddVoteGranted(b, reply.VoteGranted)
		root := pb.RequestVoteReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeAppendEntries, metaRPCAppendEntries:
		args := msg.(*AppendEntriesArgs)
		b := raftBuilderPool.Get()
		root := buildAppendEntriesArgsFlatBuffer(b, args)
		return fbFinishRPC(b, root), nil

	case rpcTypeAppendEntriesReply, metaRPCAppendEntriesReply:
		reply := msg.(*AppendEntriesReply)
		b := raftBuilderPool.Get()
		pb.AppendEntriesReplyStart(b)
		pb.AppendEntriesReplyAddTerm(b, reply.Term)
		pb.AppendEntriesReplyAddSuccess(b, reply.Success)
		pb.AppendEntriesReplyAddConflictTerm(b, reply.ConflictTerm)
		pb.AppendEntriesReplyAddConflictIndex(b, reply.ConflictIndex)
		root := pb.AppendEntriesReplyEnd(b)
		return fbFinishRPC(b, root), nil

	case rpcTypeInstallSnapshot, metaRPCInstallSnapshot:
		args := msg.(*InstallSnapshotArgs)
		b := raftBuilderPool.Get()

		servers := args.Servers
		if len(servers) == 0 {
			servers = make([]Server, 0, len(args.Configuration)+len(args.Learners))
			for _, id := range args.Configuration {
				servers = append(servers, Server{ID: id, Suffrage: Voter})
			}
			for id := range args.Learners {
				servers = append(servers, Server{ID: id, Suffrage: NonVoter})
			}
		}

		// Build ServerEntry objects before Start (FlatBuffers reverse-order rule)
		serverOffs := make([]flatbuffers.UOffsetT, len(servers))
		for i := len(servers) - 1; i >= 0; i-- {
			s := servers[i]
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

	case rpcTypeInstallSnapshotReply, metaRPCInstallSnapshotReply:
		reply := msg.(*InstallSnapshotReply)
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

func decodeRequestVoteArgs(data []byte) (args *RequestVoteArgs, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode RequestVoteArgs: invalid flatbuffer: %v", r)
		}
	}()
	a := pb.GetRootAsRequestVoteArgs(data, 0)
	return &RequestVoteArgs{
		Term:           a.Term(),
		CandidateID:    string(a.CandidateId()),
		LastLogIndex:   a.LastLogIndex(),
		LastLogTerm:    a.LastLogTerm(),
		PreVote:        a.PreVote(),
		LeaderTransfer: a.LeaderTransfer(),
	}, nil
}

func decodeRequestVoteReply(data []byte) (reply *RequestVoteReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode RequestVoteReply: invalid flatbuffer: %v", r)
		}
	}()
	r := pb.GetRootAsRequestVoteReply(data, 0)
	return &RequestVoteReply{
		Term:        r.Term(),
		VoteGranted: r.VoteGranted(),
	}, nil
}

func decodeAppendEntriesArgs(data []byte) (args *AppendEntriesArgs, err error) {
	args = new(AppendEntriesArgs)
	if err := decodeAppendEntriesArgsInto(data, args, nil); err != nil {
		return nil, err
	}
	return args, nil
}

type appendEntriesDecodeStringCache struct {
	leaderID string
}

func (c *appendEntriesDecodeStringCache) copyLeaderID(b []byte) string {
	if c != nil && bytesEqualString(c.leaderID, b) {
		return c.leaderID
	}
	s := string(b)
	if c != nil {
		c.leaderID = s
	}
	return s
}

func bytesEqualString(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := range b {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

func decodeAppendEntriesArgsInto(data []byte, dst *AppendEntriesArgs, strings *appendEntriesDecodeStringCache) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode AppendEntriesArgs: invalid flatbuffer: %v", r)
		}
	}()
	a := pb.GetRootAsAppendEntriesArgs(data, 0)
	entries := make([]LogEntry, a.EntriesLength())
	var e pb.LogEntry
	for i := 0; i < a.EntriesLength(); i++ {
		if !a.Entries(&e, i) {
			continue
		}
		entries[i] = LogEntry{Term: e.Term(), Index: e.Index(), Command: e.CommandBytes(), Type: LogEntryType(e.EntryType())}
	}
	*dst = AppendEntriesArgs{
		Term:         a.Term(),
		LeaderID:     strings.copyLeaderID(a.LeaderId()),
		PrevLogIndex: a.PrevLogIndex(),
		PrevLogTerm:  a.PrevLogTerm(),
		Entries:      entries,
		LeaderCommit: a.LeaderCommit(),
	}
	return nil
}

func decodeAppendEntriesReply(data []byte) (reply *AppendEntriesReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode AppendEntriesReply: invalid flatbuffer: %v", r)
		}
	}()
	r := pb.GetRootAsAppendEntriesReply(data, 0)
	return &AppendEntriesReply{
		Term:          r.Term(),
		Success:       r.Success(),
		ConflictTerm:  r.ConflictTerm(),
		ConflictIndex: r.ConflictIndex(),
	}, nil
}

func decodeInstallSnapshotArgs(data []byte) (args *InstallSnapshotArgs, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode InstallSnapshotArgs: invalid flatbuffer: %v", r)
		}
	}()
	a := pb.GetRootAsInstallSnapshotArgs(data, 0)
	servers := make([]Server, 0, a.ServersLength())
	cfg := make([]string, 0, a.ServersLength())
	learners := make(map[string]string)
	var se pb.ServerEntry
	for i := 0; i < a.ServersLength(); i++ {
		if a.Servers(&se, i) {
			id := string(se.Id())
			suffrage := ServerSuffrage(se.Suffrage())
			servers = append(servers, Server{ID: id, Suffrage: suffrage})
			if suffrage == NonVoter {
				learners[id] = ""
				continue
			}
			cfg = append(cfg, id)
		}
	}
	if len(learners) == 0 {
		learners = nil
	}
	return &InstallSnapshotArgs{
		Term:              a.Term(),
		LeaderID:          string(a.LeaderId()),
		LastIncludedIndex: a.LastIncludedIndex(),
		LastIncludedTerm:  a.LastIncludedTerm(),
		Data:              a.DataBytes(),
		Servers:           servers,
		Configuration:     cfg,
		Learners:          learners,
	}, nil
}

func decodeInstallSnapshotReply(data []byte) (reply *InstallSnapshotReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode InstallSnapshotReply: invalid flatbuffer: %v", r)
		}
	}()
	r := pb.GetRootAsInstallSnapshotReply(data, 0)
	return &InstallSnapshotReply{
		Term: r.Term(),
	}, nil
}
