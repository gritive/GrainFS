package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

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

func (r forwardRuntime) mutateFrame(ctx context.Context, target RouteTarget, op raftpb.ForwardOp, args []byte) error {
	if r.sender == nil {
		return ErrCoordinatorNoRouter
	}
	reply, err := r.sender.Send(ctx, target.Peers, target.GroupID, op, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
}

func (r forwardRuntime) deleteObject(ctx context.Context, target RouteTarget, args []byte) (string, error) {
	if r.sender == nil {
		return "", ErrCoordinatorNoRouter
	}
	reply, err := r.sender.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpDeleteObject, args)
	if err != nil {
		return "", err
	}
	obj, err := objectFromReply(reply)
	if err == nil {
		return obj.VersionID, nil
	}
	if errors.Is(err, errInternalReply) {
		return "", parseReplyStatus(reply)
	}
	return "", err
}

func (r forwardRuntime) putObject(
	ctx context.Context,
	target RouteTarget,
	group ShardGroupEntry,
	req storage.PutObjectRequest,
	routeStart time.Time,
) (*storage.Object, error) {
	if r.sender == nil {
		return nil, ErrCoordinatorNoRouter
	}
	bucket, key := req.Bucket, req.Key
	bodyReader := req.Body
	contentType := req.ContentType
	if r.sender.streamDialer != nil && forwardBodyExceedsSingleFrameCap(bodyReader, r.maxBody) {
		args := buildPutObjectArgsWithSSE(bucket, key, contentType, nil, req.SystemMetadata.SSEAlgorithm)
		ctx = ContextWithPutTrace(ctx, PutTraceRequest{
			Bucket:      bucket,
			Key:         key,
			GroupID:     target.GroupID,
			Ingress:     PutTraceIngressForwardedNonLeader,
			SizeClass:   PutTraceSizeLarge,
			ForwardMode: PutTraceForwardStream,
		})
		ObservePutTraceStage(ctx, PutTraceStageRouteWrite, routeStart, PutTraceStageFields{})
		resolveStart := time.Now()
		peers := r.sender.ResolveLeaderPeers(ctx, target.Peers, target.GroupID, bucket, key)
		ObservePutTraceStage(ctx, PutTraceStageForwardResolveLeader, resolveStart, PutTraceStageFields{})
		reply, err := r.sender.SendStream(ctx, peers, target.GroupID, raftpb.ForwardOpPutObject, args, bodyReader)
		if err != nil {
			return nil, topologyForwardWriteError(group, err)
		}
		obj, err := objectFromReply(reply)
		if err != nil {
			logForwardReplyDecodeError(err, bucket, key, target.GroupID, raftpb.ForwardOpPutObject, reply)
			return nil, err
		}
		return obj, nil
	}

	body, err := readBoundedBody(bodyReader, r.maxBody)
	if err != nil {
		return nil, err
	}
	args := buildPutObjectArgsWithSSE(bucket, key, contentType, body, req.SystemMetadata.SSEAlgorithm)
	ctx = ContextWithPutTrace(ctx, PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		GroupID:     target.GroupID,
		Ingress:     PutTraceIngressForwardedNonLeader,
		SizeClass:   putTraceSizeClass(int64(len(body)), r.maxBody),
		ForwardMode: PutTraceForwardFrame,
	})
	ObservePutTraceStage(ctx, PutTraceStageRouteWrite, routeStart, PutTraceStageFields{})
	resolveStart := time.Now()
	peers := r.sender.ResolveLeaderPeers(ctx, target.Peers, target.GroupID, bucket, key)
	ObservePutTraceStage(ctx, PutTraceStageForwardResolveLeader, resolveStart, PutTraceStageFields{})
	reply, err := r.sender.Send(ctx, peers, target.GroupID, raftpb.ForwardOpPutObject, args)
	if err != nil {
		return nil, topologyForwardWriteError(group, err)
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		logForwardReplyDecodeError(err, bucket, key, target.GroupID, raftpb.ForwardOpPutObject, reply)
		return nil, err
	}
	if obj.Size != int64(len(body)) {
		return nil, ErrForwardBodySizeMismatch
	}
	return obj, nil
}

func (r forwardRuntime) uploadPart(
	ctx context.Context,
	target RouteTarget,
	bucket, key, uploadID string,
	partNumber int,
	bodyReader io.Reader,
) (*storage.Part, error) {
	if r.sender == nil {
		return nil, ErrCoordinatorNoRouter
	}
	if r.sender.streamDialer != nil && shouldStreamUploadPartForward(bodyReader, r.maxBody) {
		args := buildUploadPartArgs(bucket, key, uploadID, int32(partNumber), nil)
		peers := r.sender.ResolveLeaderPeers(ctx, target.Peers, target.GroupID, bucket, key)
		reply, err := r.sender.SendStream(ctx, peers, target.GroupID, raftpb.ForwardOpUploadPart, args, bodyReader)
		if err != nil {
			return nil, err
		}
		return partFromReply(reply)
	}

	body, err := forwardBodyBytes(bodyReader, r.maxBody)
	if err != nil {
		return nil, err
	}
	args := buildUploadPartArgs(bucket, key, uploadID, int32(partNumber), body)
	reply, err := r.sender.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpUploadPart, args)
	if err != nil {
		return nil, err
	}
	part, err := partFromReply(reply)
	if err != nil {
		return nil, err
	}
	if part.Size != int64(len(body)) {
		return nil, ErrForwardBodySizeMismatch
	}
	return part, nil
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
