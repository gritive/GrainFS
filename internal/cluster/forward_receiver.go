package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/rs/zerolog/log"
)

type ForwardReceiver struct {
	groups *DataGroupManager
}

func NewForwardReceiver(groups *DataGroupManager) *ForwardReceiver {
	return &ForwardReceiver{groups: groups}
}

// Register installs this ForwardReceiver as the handler for StreamProposeGroupForward (0x08) on shardSvc.
// The 0x08 stream type is used for intra-cluster forwarding of bucket-scoped operations.
func (r *ForwardReceiver) Register(shardSvc *ShardService) {
	shardSvc.RegisterHandler(transport.StreamProposeGroupForward, r.Handle)
	shardSvc.RegisterBodyHandler(transport.StreamGroupForwardBody, r.HandleBody)
	shardSvc.RegisterReadHandler(transport.StreamGroupForwardRead, r.HandleRead)
}

// Handle implements transport.Handler for 0x08 stream.
func (r *ForwardReceiver) Handle(req *transport.Message) *transport.Message {
	groupID, op, fbsArgs, err := decodeForwardPayload(req.Payload)
	if err != nil {
		return errReply(raftpb.ForwardStatusInternal, "")
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		return errReply(raftpb.ForwardStatusNotVoter, "")
	}

	node := dg.Backend().RaftNode()
	if node == nil || !node.IsLeader() {
		hint := ""
		if node != nil {
			hint = node.LeaderID()
		}
		return errReply(raftpb.ForwardStatusNotLeader, hint)
	}

	switch op {
	case raftpb.ForwardOpPutObject:
		return r.handlePutObject(dg, fbsArgs)
	case raftpb.ForwardOpGetObject:
		return r.handleGetObject(dg, fbsArgs)
	case raftpb.ForwardOpHeadObject:
		return r.handleHeadObject(dg, fbsArgs)
	case raftpb.ForwardOpDeleteObject:
		return r.handleDeleteObject(dg, fbsArgs)
	case raftpb.ForwardOpListObjects:
		return r.handleListObjects(dg, fbsArgs)
	case raftpb.ForwardOpWalkObjects:
		return r.handleWalkObjects(dg, fbsArgs)
	case raftpb.ForwardOpCreateMultipartUpload:
		return r.handleCreateMultipartUpload(dg, fbsArgs)
	case raftpb.ForwardOpUploadPart:
		return r.handleUploadPart(dg, fbsArgs)
	case raftpb.ForwardOpCompleteMultipartUpload:
		return r.handleCompleteMultipartUpload(dg, fbsArgs)
	case raftpb.ForwardOpAbortMultipartUpload:
		return r.handleAbortMultipartUpload(dg, fbsArgs)
	case raftpb.ForwardOpGetObjectVersion:
		return r.handleGetObjectVersion(dg, fbsArgs)
	case raftpb.ForwardOpDeleteObjectVersion:
		return r.handleDeleteObjectVersion(dg, fbsArgs)
	case raftpb.ForwardOpListObjectVersions:
		return r.handleListObjectVersions(dg, fbsArgs)
	default:
		return errReply(raftpb.ForwardStatusInternal, "")
	}
}

// HandleBody implements streamed-body forwarding for PutObject and UploadPart.
// The request payload carries group/op/metadata; body bytes follow the frame on
// the same QUIC stream and are passed directly into the local GroupBackend.
func (r *ForwardReceiver) HandleBody(req *transport.Message, body io.Reader) *transport.Message {
	groupID, op, fbsArgs, err := decodeForwardPayload(req.Payload)
	if err != nil {
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusInternal, "")
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusNotVoter, "")
	}

	node := dg.Backend().RaftNode()
	if node == nil || !node.IsLeader() {
		drainForwardBody(body)
		hint := ""
		if node != nil {
			hint = node.LeaderID()
		}
		return errReply(raftpb.ForwardStatusNotLeader, hint)
	}

	switch op {
	case raftpb.ForwardOpPutObject:
		return r.handlePutObjectStream(dg, fbsArgs, body)
	case raftpb.ForwardOpUploadPart:
		return r.handleUploadPartStream(dg, fbsArgs, body)
	default:
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusInternal, "")
	}
}

// HandleRead implements streamed-response forwarding for GetObject and
// GetObjectVersion. The returned ForwardReply carries metadata only; object
// bytes follow as the raw response body on the same QUIC stream.
func (r *ForwardReceiver) HandleRead(req *transport.Message) (*transport.Message, io.ReadCloser) {
	groupID, op, fbsArgs, err := decodeForwardPayload(req.Payload)
	if err != nil {
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		return errReply(raftpb.ForwardStatusNotVoter, ""), nil
	}

	node := dg.Backend().RaftNode()
	if node == nil || !node.IsLeader() {
		hint := ""
		if node != nil {
			hint = node.LeaderID()
		}
		return errReply(raftpb.ForwardStatusNotLeader, hint), nil
	}

	switch op {
	case raftpb.ForwardOpGetObject:
		return r.handleGetObjectRead(dg, fbsArgs)
	case raftpb.ForwardOpGetObjectVersion:
		return r.handleGetObjectVersionRead(dg, fbsArgs)
	default:
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}
}

func drainForwardBody(body io.Reader) {
	if body != nil {
		_, _ = io.Copy(io.Discard, body)
	}
}

func (r *ForwardReceiver) handlePutObject(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	body := pa.BodyBytes()
	obj, err := dg.Backend().PutObject(
		ctx,
		string(pa.Bucket()),
		string(pa.Key()),
		bytes.NewReader(body),
		string(pa.ContentType()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, string(pa.Bucket()))}
}

func (r *ForwardReceiver) handlePutObjectStream(dg *DataGroup, args []byte, body io.Reader) *transport.Message {
	ctx := context.Background()
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	obj, err := dg.Backend().PutObject(
		ctx,
		string(pa.Bucket()),
		string(pa.Key()),
		body,
		string(pa.ContentType()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, string(pa.Bucket()))}
}

func (r *ForwardReceiver) handleGetObjectRead(dg *DataGroup, args []byte) (*transport.Message, io.ReadCloser) {
	ctx := context.Background()
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	rc, obj, err := dg.Backend().GetObject(ctx, string(ga.Bucket()), string(ga.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	return &transport.Message{Payload: buildGetObjectReply(obj, string(ga.Bucket()), nil)}, rc
}

func (r *ForwardReceiver) handleGetObject(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	rc, obj, err := dg.Backend().GetObject(ctx, string(ga.Bucket()), string(ga.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, DefaultMaxForwardReplyBytes+1))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	if int64(len(body)) > DefaultMaxForwardReplyBytes {
		return statusReply(raftpb.ForwardStatusEntityTooLarge)
	}
	if obj.Size != int64(len(body)) {
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildGetObjectReply(obj, string(ga.Bucket()), body)}
}

func (r *ForwardReceiver) handleGetObjectVersionRead(dg *DataGroup, args []byte) (*transport.Message, io.ReadCloser) {
	ga := raftpb.GetRootAsGetObjectVersionArgs(args, 0)
	rc, obj, err := dg.Backend().GetObjectVersion(string(ga.Bucket()), string(ga.Key()), string(ga.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	return &transport.Message{Payload: buildGetObjectReply(obj, string(ga.Bucket()), nil)}, rc
}

func (r *ForwardReceiver) handleGetObjectVersion(dg *DataGroup, args []byte) *transport.Message {
	ga := raftpb.GetRootAsGetObjectVersionArgs(args, 0)
	rc, obj, err := dg.Backend().GetObjectVersion(string(ga.Bucket()), string(ga.Key()), string(ga.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, DefaultMaxForwardReplyBytes+1))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	if int64(len(body)) > DefaultMaxForwardReplyBytes {
		return statusReply(raftpb.ForwardStatusEntityTooLarge)
	}
	if obj.Size != int64(len(body)) {
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildGetObjectReply(obj, string(ga.Bucket()), body)}
}

func (r *ForwardReceiver) handleHeadObject(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	ha := raftpb.GetRootAsHeadObjectArgs(args, 0)
	obj, err := dg.Backend().HeadObject(ctx, string(ha.Bucket()), string(ha.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, string(ha.Bucket()))}
}

func (r *ForwardReceiver) handleDeleteObject(dg *DataGroup, args []byte) *transport.Message {
	da := raftpb.GetRootAsDeleteObjectArgs(args, 0)
	bucket := string(da.Bucket())
	key := string(da.Key())
	markerID, err := dg.Backend().DeleteObjectReturningMarker(bucket, key)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(&storage.Object{
		Key:       key,
		VersionID: markerID,
	}, bucket)}
}

func (r *ForwardReceiver) handleDeleteObjectVersion(dg *DataGroup, args []byte) *transport.Message {
	da := raftpb.GetRootAsDeleteObjectVersionArgs(args, 0)
	err := dg.Backend().DeleteObjectVersion(string(da.Bucket()), string(da.Key()), string(da.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildOKReply()}
}

func (r *ForwardReceiver) handleListObjects(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	la := raftpb.GetRootAsListObjectsArgs(args, 0)
	objs, err := dg.Backend().ListObjects(ctx, string(la.Bucket()), string(la.Prefix()), int(la.MaxKeys()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectsReply(string(la.Bucket()), objs)}
}

func (r *ForwardReceiver) handleListObjectVersions(dg *DataGroup, args []byte) *transport.Message {
	la := raftpb.GetRootAsListObjectVersionsArgs(args, 0)
	versions, err := dg.Backend().ListObjectVersions(string(la.Bucket()), string(la.Prefix()), int(la.MaxKeys()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectVersionsReply(versions)}
}

func (r *ForwardReceiver) handleWalkObjects(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	wa := raftpb.GetRootAsWalkObjectsArgs(args, 0)
	var objs []*storage.Object
	err := dg.Backend().WalkObjects(ctx, string(wa.Bucket()), string(wa.Prefix()), func(o *storage.Object) error {
		objs = append(objs, o)
		return nil
	})
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectsReply(string(wa.Bucket()), objs)}
}

func (r *ForwardReceiver) handleCreateMultipartUpload(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	ca := raftpb.GetRootAsCreateMultipartUploadArgs(args, 0)
	upload, err := dg.Backend().CreateMultipartUpload(
		ctx,
		string(ca.Bucket()),
		string(ca.Key()),
		string(ca.ContentType()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildUploadReply(upload.Bucket, upload.Key, upload.UploadID)}
}

func (r *ForwardReceiver) handleUploadPart(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	ua := raftpb.GetRootAsUploadPartArgs(args, 0)
	body := ua.BodyBytes()
	part, err := dg.Backend().UploadPart(
		ctx,
		string(ua.Bucket()),
		string(ua.Key()),
		string(ua.UploadId()),
		int(ua.PartNumber()),
		bytes.NewReader(body),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildPartReply(part)}
}

func (r *ForwardReceiver) handleUploadPartStream(dg *DataGroup, args []byte, body io.Reader) *transport.Message {
	ctx := context.Background()
	ua := raftpb.GetRootAsUploadPartArgs(args, 0)
	part, err := dg.Backend().UploadPart(
		ctx,
		string(ua.Bucket()),
		string(ua.Key()),
		string(ua.UploadId()),
		int(ua.PartNumber()),
		body,
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildPartReply(part)}
}

func (r *ForwardReceiver) handleCompleteMultipartUpload(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	ca := raftpb.GetRootAsCompleteMultipartUploadArgs(args, 0)
	n := ca.PartsLength()
	parts := make([]storage.Part, n)
	var partRef raftpb.PartRef
	for i := 0; i < n; i++ {
		if ca.Parts(&partRef, i) {
			parts[i] = storage.Part{
				PartNumber: int(partRef.PartNumber()),
				ETag:       string(partRef.Etag()),
			}
		}
	}
	obj, err := dg.Backend().CompleteMultipartUpload(
		ctx,
		string(ca.Bucket()),
		string(ca.Key()),
		string(ca.UploadId()),
		parts,
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, string(ca.Bucket()))}
}

func (r *ForwardReceiver) handleAbortMultipartUpload(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	aa := raftpb.GetRootAsAbortMultipartUploadArgs(args, 0)
	err := dg.Backend().AbortMultipartUpload(
		ctx,
		string(aa.Bucket()),
		string(aa.Key()),
		string(aa.UploadId()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildOKReply()}
}

func errReply(status raftpb.ForwardStatus, hint string) *transport.Message {
	return &transport.Message{Payload: buildSimpleReply(status, hint)}
}

func statusReply(status raftpb.ForwardStatus) *transport.Message {
	return errReply(status, "")
}

func mapErrorToStatus(err error) raftpb.ForwardStatus {
	if err == nil {
		return raftpb.ForwardStatusOK
	}
	if errors.Is(err, storage.ErrNoSuchBucket) {
		return raftpb.ForwardStatusNoSuchBucket
	}
	if errors.Is(err, storage.ErrObjectNotFound) {
		return raftpb.ForwardStatusNoSuchKey
	}
	if errors.Is(err, storage.ErrEntityTooLarge) {
		return raftpb.ForwardStatusEntityTooLarge
	}
	log.Warn().Err(err).Msg("forward receiver mapped backend error to internal status")
	return raftpb.ForwardStatusInternal
}
