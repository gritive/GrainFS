package cluster

import (
	"bytes"
	"io"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
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
	default:
		return errReply(raftpb.ForwardStatusInternal, "")
	}
}

func (r *ForwardReceiver) handlePutObject(dg *DataGroup, args []byte) *transport.Message {
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	body := pa.BodyBytes()
	obj, err := dg.Backend().PutObject(
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

func (r *ForwardReceiver) handleGetObject(dg *DataGroup, args []byte) *transport.Message {
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	rc, obj, err := dg.Backend().GetObject(string(ga.Bucket()), string(ga.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildGetObjectReply(obj, string(ga.Bucket()), body)}
}

func (r *ForwardReceiver) handleHeadObject(dg *DataGroup, args []byte) *transport.Message {
	ha := raftpb.GetRootAsHeadObjectArgs(args, 0)
	obj, err := dg.Backend().HeadObject(string(ha.Bucket()), string(ha.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, string(ha.Bucket()))}
}

func (r *ForwardReceiver) handleDeleteObject(dg *DataGroup, args []byte) *transport.Message {
	da := raftpb.GetRootAsDeleteObjectArgs(args, 0)
	err := dg.Backend().DeleteObject(string(da.Bucket()), string(da.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildOKReply()}
}

func (r *ForwardReceiver) handleListObjects(dg *DataGroup, args []byte) *transport.Message {
	la := raftpb.GetRootAsListObjectsArgs(args, 0)
	objs, err := dg.Backend().ListObjects(string(la.Bucket()), string(la.Prefix()), int(la.MaxKeys()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectsReply(string(la.Bucket()), objs)}
}

func (r *ForwardReceiver) handleWalkObjects(dg *DataGroup, args []byte) *transport.Message {
	wa := raftpb.GetRootAsWalkObjectsArgs(args, 0)
	var objs []*storage.Object
	err := dg.Backend().WalkObjects(string(wa.Bucket()), string(wa.Prefix()), func(o *storage.Object) error {
		objs = append(objs, o)
		return nil
	})
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectsReply(string(wa.Bucket()), objs)}
}

func (r *ForwardReceiver) handleCreateMultipartUpload(dg *DataGroup, args []byte) *transport.Message {
	ca := raftpb.GetRootAsCreateMultipartUploadArgs(args, 0)
	upload, err := dg.Backend().CreateMultipartUpload(
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
	ua := raftpb.GetRootAsUploadPartArgs(args, 0)
	body := ua.BodyBytes()
	part, err := dg.Backend().UploadPart(
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

func (r *ForwardReceiver) handleCompleteMultipartUpload(dg *DataGroup, args []byte) *transport.Message {
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
	aa := raftpb.GetRootAsAbortMultipartUploadArgs(args, 0)
	err := dg.Backend().AbortMultipartUpload(
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
	if err == storage.ErrNoSuchBucket {
		return raftpb.ForwardStatusNoSuchBucket
	}
	if err == storage.ErrObjectNotFound {
		return raftpb.ForwardStatusNoSuchKey
	}
	if err == storage.ErrEntityTooLarge {
		return raftpb.ForwardStatusEntityTooLarge
	}
	return raftpb.ForwardStatusInternal
}
