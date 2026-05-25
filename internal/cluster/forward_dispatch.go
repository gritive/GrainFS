package cluster

import (
	"io"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
)

type forwardTransportKind uint8

const (
	forwardFrameOnly forwardTransportKind = iota
	forwardBodyStream
	forwardReadStream
)

type bucketForwardOpSpec struct {
	op          raftpb.ForwardOp
	name        string
	transport   forwardTransportKind
	mutates     bool
	handleFrame func(*ForwardReceiver, *DataGroup, []byte) *transport.Message
	handleBody  func(*ForwardReceiver, *DataGroup, []byte, io.Reader) *transport.Message
	handleRead  func(*ForwardReceiver, *DataGroup, []byte) (*transport.Message, io.ReadCloser)
}

func (s bucketForwardOpSpec) allowedOn(kind forwardTransportKind) bool {
	if kind == forwardFrameOnly {
		// The frame handler is also the legacy path for body/read operations.
		return true
	}
	return s.transport == kind
}

var bucketForwardOpSpecs = map[raftpb.ForwardOp]bucketForwardOpSpec{
	raftpb.ForwardOpPutObject: {
		op:          raftpb.ForwardOpPutObject,
		name:        raftpb.ForwardOpPutObject.String(),
		transport:   forwardBodyStream,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handlePutObject,
		handleBody:  (*ForwardReceiver).handlePutObjectStream,
	},
	raftpb.ForwardOpGetObject: {
		op:          raftpb.ForwardOpGetObject,
		name:        raftpb.ForwardOpGetObject.String(),
		transport:   forwardReadStream,
		handleFrame: (*ForwardReceiver).handleGetObject,
		handleRead:  (*ForwardReceiver).handleGetObjectRead,
	},
	raftpb.ForwardOpHeadObject: {
		op:          raftpb.ForwardOpHeadObject,
		name:        raftpb.ForwardOpHeadObject.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleHeadObject,
	},
	raftpb.ForwardOpDeleteObject: {
		op:          raftpb.ForwardOpDeleteObject,
		name:        raftpb.ForwardOpDeleteObject.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleDeleteObject,
	},
	raftpb.ForwardOpSetObjectACL: {
		op:          raftpb.ForwardOpSetObjectACL,
		name:        raftpb.ForwardOpSetObjectACL.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleSetObjectACL,
	},
	raftpb.ForwardOpSetObjectTags: {
		op:          raftpb.ForwardOpSetObjectTags,
		name:        raftpb.ForwardOpSetObjectTags.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleSetObjectTags,
	},
	raftpb.ForwardOpGetObjectTags: {
		op:          raftpb.ForwardOpGetObjectTags,
		name:        raftpb.ForwardOpGetObjectTags.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleGetObjectTags,
	},
	raftpb.ForwardOpListObjects: {
		op:          raftpb.ForwardOpListObjects,
		name:        raftpb.ForwardOpListObjects.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleListObjects,
	},
	raftpb.ForwardOpWalkObjects: {
		op:          raftpb.ForwardOpWalkObjects,
		name:        raftpb.ForwardOpWalkObjects.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleWalkObjects,
	},
	raftpb.ForwardOpCreateMultipartUpload: {
		op:          raftpb.ForwardOpCreateMultipartUpload,
		name:        raftpb.ForwardOpCreateMultipartUpload.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleCreateMultipartUpload,
	},
	raftpb.ForwardOpUploadPart: {
		op:          raftpb.ForwardOpUploadPart,
		name:        raftpb.ForwardOpUploadPart.String(),
		transport:   forwardBodyStream,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleUploadPart,
		handleBody:  (*ForwardReceiver).handleUploadPartStream,
	},
	raftpb.ForwardOpCompleteMultipartUpload: {
		op:          raftpb.ForwardOpCompleteMultipartUpload,
		name:        raftpb.ForwardOpCompleteMultipartUpload.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleCompleteMultipartUpload,
	},
	raftpb.ForwardOpAbortMultipartUpload: {
		op:          raftpb.ForwardOpAbortMultipartUpload,
		name:        raftpb.ForwardOpAbortMultipartUpload.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleAbortMultipartUpload,
	},
	raftpb.ForwardOpListParts: {
		op:          raftpb.ForwardOpListParts,
		name:        raftpb.ForwardOpListParts.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleListParts,
	},
	raftpb.ForwardOpListMultipartUploads: {
		op:          raftpb.ForwardOpListMultipartUploads,
		name:        raftpb.ForwardOpListMultipartUploads.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleListMultipartUploads,
	},
	raftpb.ForwardOpGetObjectVersion: {
		op:          raftpb.ForwardOpGetObjectVersion,
		name:        raftpb.ForwardOpGetObjectVersion.String(),
		transport:   forwardReadStream,
		handleFrame: (*ForwardReceiver).handleGetObjectVersion,
		handleRead:  (*ForwardReceiver).handleGetObjectVersionRead,
	},
	raftpb.ForwardOpDeleteObjectVersion: {
		op:          raftpb.ForwardOpDeleteObjectVersion,
		name:        raftpb.ForwardOpDeleteObjectVersion.String(),
		transport:   forwardFrameOnly,
		mutates:     true,
		handleFrame: (*ForwardReceiver).handleDeleteObjectVersion,
	},
	raftpb.ForwardOpListObjectVersions: {
		op:          raftpb.ForwardOpListObjectVersions,
		name:        raftpb.ForwardOpListObjectVersions.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleListObjectVersions,
	},
	raftpb.ForwardOpHeadObjectVersion: {
		op:          raftpb.ForwardOpHeadObjectVersion,
		name:        raftpb.ForwardOpHeadObjectVersion.String(),
		transport:   forwardFrameOnly,
		handleFrame: (*ForwardReceiver).handleHeadObjectVersion,
	},
	raftpb.ForwardOpReadAt: {
		op:          raftpb.ForwardOpReadAt,
		name:        raftpb.ForwardOpReadAt.String(),
		transport:   forwardReadStream,
		handleFrame: (*ForwardReceiver).handleReadAt,
		handleRead:  (*ForwardReceiver).handleReadAtRead,
	},
	raftpb.ForwardOpAppendObject: {
		op:         raftpb.ForwardOpAppendObject,
		name:       raftpb.ForwardOpAppendObject.String(),
		transport:  forwardBodyStream,
		mutates:    true,
		handleBody: (*ForwardReceiver).handleAppendObjectStream,
	},
}

func lookupBucketForwardOpSpec(op raftpb.ForwardOp) (bucketForwardOpSpec, bool) {
	spec, ok := bucketForwardOpSpecs[op]
	return spec, ok
}
