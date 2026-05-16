package cluster

import "github.com/gritive/GrainFS/internal/raft/raftpb"

type forwardTransportKind uint8

const (
	forwardFrameOnly forwardTransportKind = iota
	forwardBodyStream
	forwardReadStream
)

type bucketForwardOpSpec struct {
	op        raftpb.ForwardOp
	name      string
	transport forwardTransportKind
	mutates   bool
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
		op:        raftpb.ForwardOpPutObject,
		name:      raftpb.ForwardOpPutObject.String(),
		transport: forwardBodyStream,
		mutates:   true,
	},
	raftpb.ForwardOpGetObject: {
		op:        raftpb.ForwardOpGetObject,
		name:      raftpb.ForwardOpGetObject.String(),
		transport: forwardReadStream,
	},
	raftpb.ForwardOpHeadObject: {
		op:        raftpb.ForwardOpHeadObject,
		name:      raftpb.ForwardOpHeadObject.String(),
		transport: forwardFrameOnly,
	},
	raftpb.ForwardOpDeleteObject: {
		op:        raftpb.ForwardOpDeleteObject,
		name:      raftpb.ForwardOpDeleteObject.String(),
		transport: forwardFrameOnly,
		mutates:   true,
	},
	raftpb.ForwardOpSetObjectACL: {
		op:        raftpb.ForwardOpSetObjectACL,
		name:      raftpb.ForwardOpSetObjectACL.String(),
		transport: forwardFrameOnly,
		mutates:   true,
	},
	raftpb.ForwardOpListObjects: {
		op:        raftpb.ForwardOpListObjects,
		name:      raftpb.ForwardOpListObjects.String(),
		transport: forwardFrameOnly,
	},
	raftpb.ForwardOpWalkObjects: {
		op:        raftpb.ForwardOpWalkObjects,
		name:      raftpb.ForwardOpWalkObjects.String(),
		transport: forwardFrameOnly,
	},
	raftpb.ForwardOpCreateMultipartUpload: {
		op:        raftpb.ForwardOpCreateMultipartUpload,
		name:      raftpb.ForwardOpCreateMultipartUpload.String(),
		transport: forwardFrameOnly,
		mutates:   true,
	},
	raftpb.ForwardOpUploadPart: {
		op:        raftpb.ForwardOpUploadPart,
		name:      raftpb.ForwardOpUploadPart.String(),
		transport: forwardBodyStream,
		mutates:   true,
	},
	raftpb.ForwardOpCompleteMultipartUpload: {
		op:        raftpb.ForwardOpCompleteMultipartUpload,
		name:      raftpb.ForwardOpCompleteMultipartUpload.String(),
		transport: forwardFrameOnly,
		mutates:   true,
	},
	raftpb.ForwardOpAbortMultipartUpload: {
		op:        raftpb.ForwardOpAbortMultipartUpload,
		name:      raftpb.ForwardOpAbortMultipartUpload.String(),
		transport: forwardFrameOnly,
		mutates:   true,
	},
	raftpb.ForwardOpListParts: {
		op:        raftpb.ForwardOpListParts,
		name:      raftpb.ForwardOpListParts.String(),
		transport: forwardFrameOnly,
	},
	raftpb.ForwardOpGetObjectVersion: {
		op:        raftpb.ForwardOpGetObjectVersion,
		name:      raftpb.ForwardOpGetObjectVersion.String(),
		transport: forwardReadStream,
	},
	raftpb.ForwardOpDeleteObjectVersion: {
		op:        raftpb.ForwardOpDeleteObjectVersion,
		name:      raftpb.ForwardOpDeleteObjectVersion.String(),
		transport: forwardFrameOnly,
		mutates:   true,
	},
	raftpb.ForwardOpListObjectVersions: {
		op:        raftpb.ForwardOpListObjectVersions,
		name:      raftpb.ForwardOpListObjectVersions.String(),
		transport: forwardFrameOnly,
	},
	raftpb.ForwardOpReadAt: {
		op:        raftpb.ForwardOpReadAt,
		name:      raftpb.ForwardOpReadAt.String(),
		transport: forwardReadStream,
	},
}

func lookupBucketForwardOpSpec(op raftpb.ForwardOp) (bucketForwardOpSpec, bool) {
	spec, ok := bucketForwardOpSpecs[op]
	return spec, ok
}
