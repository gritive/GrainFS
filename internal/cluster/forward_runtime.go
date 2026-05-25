package cluster

import (
	"bytes"
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

type forwardRuntime struct {
	sender      *ForwardSender
	meta        ShardGroupSource
	addr        NodeAddressBook
	selfID      string
	selfAliases []string
	maxBody     int64
}

func (r forwardRuntime) readObject(
	ctx context.Context,
	target RouteTarget,
	op raftpb.ForwardOp,
	args []byte,
) (io.ReadCloser, *storage.Object, error) {
	if r.sender == nil {
		return nil, nil, ErrCoordinatorNoRouter
	}
	peers := r.peersForTarget(target)
	if r.sender.readDialer != nil {
		reply, body, err := r.sender.SendReadStream(ctx, peers, target.GroupID, op, args)
		if err != nil {
			return nil, nil, err
		}
		obj, err := objectFromReply(reply)
		if err != nil {
			if body != nil {
				_ = body.Close()
			}
			return nil, nil, err
		}
		return &forwardReadValidator{rc: body, want: obj.Size}, obj, nil
	}

	reply, err := r.sender.Send(ctx, peers, target.GroupID, op, args)
	if err != nil {
		return nil, nil, err
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		return nil, nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	body := fr.ReadBodyBytes()
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	if obj.Size != int64(len(bodyCopy)) {
		return nil, nil, ErrForwardBodySizeMismatch
	}
	return io.NopCloser(bytes.NewReader(bodyCopy)), obj, nil
}

func (r forwardRuntime) readAt(ctx context.Context, target RouteTarget, args []byte, buf []byte) (int, error) {
	if r.sender == nil {
		return 0, ErrCoordinatorNoRouter
	}
	if r.sender.readDialer == nil {
		return 0, ErrCoordinatorNoRouter
	}
	if int64(len(buf)) <= r.maxBody {
		reply, err := r.sender.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpReadAt, args)
		if err != nil {
			return 0, err
		}
		return readAtReplyInto(reply, buf)
	}

	reply, body, err := r.sender.SendReadStream(ctx, target.Peers, target.GroupID, raftpb.ForwardOpReadAt, args)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	if err := parseReplyStatus(reply); err != nil {
		return 0, err
	}
	return io.ReadFull(body, buf)
}

func (r forwardRuntime) peersForTarget(target RouteTarget) []string {
	if len(target.Peers) > 0 || r.meta == nil {
		return target.Peers
	}
	group, ok := r.meta.ShardGroup(target.GroupID)
	if !ok {
		return target.Peers
	}
	peers := NewShardGroupPeerSet(group).ForwardOrder(r.selfID, r.selfAliases...)
	if r.addr != nil {
		if resolved, err := ResolveNodeAddresses(r.addr, peers); err == nil {
			return resolved
		}
	}
	return peers
}

type forwardReadValidator struct {
	rc   io.ReadCloser
	want int64
	got  int64
}

func (r *forwardReadValidator) Read(p []byte) (int, error) {
	if r.got >= r.want {
		return 0, io.EOF
	}
	if remaining := r.want - r.got; int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.rc.Read(p)
	r.got += int64(n)
	if r.got == r.want {
		return n, nil
	}
	if err == io.EOF {
		return n, ErrForwardBodySizeMismatch
	}
	return n, err
}

func (r *forwardReadValidator) Close() error {
	return r.rc.Close()
}
