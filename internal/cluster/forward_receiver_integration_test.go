package cluster

import (
	"bytes"
	"context"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const testForwardReplyBytesLimit = 64 * 1024

// TestForwardReceiver_PutObject_DispatchesToBackend verifies PutObject operation
// is properly decoded and routed through the ForwardReceiver.
//
// NOTE: This test uses WrapDistributedBackend with a nil raft.Node.
// The nil node causes Handle() to return ForwardStatusNotLeader.
// Full end-to-end tests with actual leader simulation would require
// in-memory BadgerDB and Raft nodes (see backend_test.go for patterns).
var _ = Describe("Forward receiver integration", func() {
	It("putobject dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1", "node2"}, gb))

		// Encode PutObject args using FlatBuffers
		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("test-bucket")
		keyStr := builder.CreateString("test-key")
		ctStr := builder.CreateString("text/plain")
		bodyBytes := builder.CreateByteVector([]byte("test-body"))

		raftpb.PutObjectArgsStart(builder)
		raftpb.PutObjectArgsAddBucket(builder, bucketStr)
		raftpb.PutObjectArgsAddKey(builder, keyStr)
		raftpb.PutObjectArgsAddContentType(builder, ctStr)
		raftpb.PutObjectArgsAddBody(builder, bodyBytes)
		paOffset := raftpb.PutObjectArgsEnd(builder)
		builder.Finish(paOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpPutObject, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		// With nil node, expect NotLeader (group exists, backend exists, but no RaftNode)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"PutObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_GetObject_DispatchesToBackend verifies GetObject operation.
	It("getobject dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("get-bucket")
		keyStr := builder.CreateString("get-key")

		raftpb.GetObjectArgsStart(builder)
		raftpb.GetObjectArgsAddBucket(builder, bucketStr)
		raftpb.GetObjectArgsAddKey(builder, keyStr)
		gaOffset := raftpb.GetObjectArgsEnd(builder)
		builder.Finish(gaOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObject, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"GetObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	It("getobjectversion toolargereturnsentitytoolarge", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		rcv.maxForwardReplyBytes = testForwardReplyBytesLimit
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		obj, err := gb.PutObject(context.Background(), "bk", "large", bytes.NewReader(bytes.Repeat([]byte("x"), testForwardReplyBytesLimit+1)), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObjectVersion, buildGetObjectVersionArgs("bk", "large", obj.VersionID))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusEntityTooLarge))
	})

	It("getobjectversionread streamsabovereplycap", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		rcv.maxForwardReplyBytes = testForwardReplyBytesLimit
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		body := bytes.Repeat([]byte("x"), testForwardReplyBytesLimit+1)
		obj, err := gb.PutObject(context.Background(), "bk", "large", bytes.NewReader(body), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObjectVersion, buildGetObjectVersionArgs("bk", "large", obj.VersionID))
		reply, streamBody := rcv.HandleRead(&transport.Message{Type: transport.StreamGroupForwardRead, Payload: payload})
		Expect(reply).NotTo(BeNil())
		Expect(streamBody).NotTo(BeNil())
		DeferCleanup(streamBody.Close)

		gotObj, err := objectFromReply(reply.Payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(gotObj.VersionID).To(Equal(obj.VersionID))
		Expect(gotObj.Size).To(Equal(int64(len(body))))
		got, err := io.ReadAll(streamBody)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(body))
	})

	It("readatread streamsonlyrequestedrange", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		body := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), 1024)
		_, err := gb.PutObject(context.Background(), "bk", "large", bytes.NewReader(body), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpReadAt, buildReadAtArgs("bk", "large", 10, 8192))
		reply, streamBody := rcv.HandleRead(&transport.Message{Type: transport.StreamGroupForwardRead, Payload: payload})
		Expect(reply).NotTo(BeNil())
		Expect(streamBody).NotTo(BeNil())
		DeferCleanup(streamBody.Close)
		Expect(parseReplyStatus(reply.Payload)).To(Succeed())

		got, err := io.ReadAll(streamBody)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(body[10 : 10+8192]))
	})

	It("readat returnsfullsmallreply", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		body := []byte("0123456789abcdef")
		_, err := gb.PutObject(context.Background(), "bk", "small", bytes.NewReader(body), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpReadAt, buildReadAtArgs("bk", "small", 4, 6))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

		Expect(reply).NotTo(BeNil())
		Expect(parseReplyStatus(reply.Payload)).To(Succeed())
		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.ReadBodyBytes()).To(Equal([]byte("456789")))
	})

	It("readat backenderrormapstostatus", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		payload := encodeForwardPayload("g1", raftpb.ForwardOpReadAt, buildReadAtArgs("bk", "missing", 0, 8))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

		Expect(reply).NotTo(BeNil())
		Expect(parseReplyStatus(reply.Payload)).To(MatchError(storage.ErrObjectNotFound))
	})

	It("readat allowsshorteofreply", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		body := []byte("short")
		_, err := gb.PutObject(context.Background(), "bk", "small", bytes.NewReader(body), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpReadAt, buildReadAtArgs("bk", "small", 0, 128))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
		Expect(reply).NotTo(BeNil())
		Expect(parseReplyStatus(reply.Payload)).To(Succeed())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.ReadBodyBytes()).To(Equal(body))
	})

	It("listobjectversions dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		first, err := gb.PutObject(context.Background(), "bk", "k", bytes.NewReader([]byte("v1")), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = gb.PutObject(context.Background(), "bk", "k", bytes.NewReader([]byte("v2")), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(gb.DeleteObject(context.Background(), "bk", "k")).To(Succeed())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjectVersions, buildListObjectVersionsArgs("bk", "k", 100))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
		Expect(reply).NotTo(BeNil())

		versions, err := objectVersionsFromReply(reply.Payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(3))
		Expect(versions[0].IsLatest).To(BeTrue())
		Expect(versions[0].IsDeleteMarker).To(BeTrue())
		Expect(versions[2].VersionID).To(Equal(first.VersionID))
	})

	It("deleteobjectversion dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		rcv.WithObjectIndexProposer(noopObjectIndexProposer{})
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		obj, err := gb.PutObject(context.Background(), "bk", "k", bytes.NewReader([]byte("v1")), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpDeleteObjectVersion, buildDeleteObjectVersionArgs("bk", "k", obj.VersionID))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
		Expect(reply).NotTo(BeNil())
		Expect(parseReplyStatus(reply.Payload)).To(Succeed())

		_, _, err = gb.GetObjectVersion("bk", "k", obj.VersionID)
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("getobjectversion dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")
		gb := newTestGroupBackend(t, "g1")
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		obj, err := gb.PutObject(context.Background(), "bk", "k", bytes.NewReader([]byte("v1")), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObjectVersion, buildGetObjectVersionArgs("bk", "k", obj.VersionID))
		reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
		Expect(reply).NotTo(BeNil())

		got, err := objectFromReply(reply.Payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.VersionID).To(Equal(obj.VersionID))
		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		body, err := io.ReadAll(bytes.NewReader(fr.ReadBodyBytes()))
		Expect(err).NotTo(HaveOccurred())
		Expect(body).To(Equal([]byte("v1")))
	})

	// TestForwardReceiver_HeadObject_DispatchesToBackend verifies HeadObject operation.
	It("headobject dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("head-bucket")
		keyStr := builder.CreateString("head-key")

		raftpb.HeadObjectArgsStart(builder)
		raftpb.HeadObjectArgsAddBucket(builder, bucketStr)
		raftpb.HeadObjectArgsAddKey(builder, keyStr)
		haOffset := raftpb.HeadObjectArgsEnd(builder)
		builder.Finish(haOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"HeadObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_HeadObjectVersion_DispatchesToBackend verifies the new
	// op decodes and routes through DataGroup.Backend; nil RaftNode yields NotLeader.
	It("headobjectversion dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObjectVersion, buildHeadObjectVersionArgs("hv-bucket", "hv-key", "vid-1"))
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"HeadObjectVersion should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_DeleteObject_DispatchesToBackend verifies DeleteObject operation.
	It("deleteobject dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("del-bucket")
		keyStr := builder.CreateString("del-key")

		raftpb.DeleteObjectArgsStart(builder)
		raftpb.DeleteObjectArgsAddBucket(builder, bucketStr)
		raftpb.DeleteObjectArgsAddKey(builder, keyStr)
		daOffset := raftpb.DeleteObjectArgsEnd(builder)
		builder.Finish(daOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpDeleteObject, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"DeleteObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_ListObjects_DispatchesToBackend verifies ListObjects operation.
	It("listobjects dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("list-bucket")
		prefixStr := builder.CreateString("prefix/")

		raftpb.ListObjectsArgsStart(builder)
		raftpb.ListObjectsArgsAddBucket(builder, bucketStr)
		raftpb.ListObjectsArgsAddPrefix(builder, prefixStr)
		raftpb.ListObjectsArgsAddMaxKeys(builder, 100)
		laOffset := raftpb.ListObjectsArgsEnd(builder)
		builder.Finish(laOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjects, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"ListObjects should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_WalkObjects_DispatchesToBackend verifies WalkObjects operation.
	It("walkobjects dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("walk-bucket")
		prefixStr := builder.CreateString("walk-prefix/")

		raftpb.WalkObjectsArgsStart(builder)
		raftpb.WalkObjectsArgsAddBucket(builder, bucketStr)
		raftpb.WalkObjectsArgsAddPrefix(builder, prefixStr)
		waOffset := raftpb.WalkObjectsArgsEnd(builder)
		builder.Finish(waOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpWalkObjects, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"WalkObjects should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_CreateMultipartUpload_DispatchesToBackend verifies CreateMultipartUpload operation.
	It("createmultipartupload dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("mpu-bucket")
		keyStr := builder.CreateString("mpu-key")
		ctStr := builder.CreateString("application/octet-stream")

		raftpb.CreateMultipartUploadArgsStart(builder)
		raftpb.CreateMultipartUploadArgsAddBucket(builder, bucketStr)
		raftpb.CreateMultipartUploadArgsAddKey(builder, keyStr)
		raftpb.CreateMultipartUploadArgsAddContentType(builder, ctStr)
		cmaOffset := raftpb.CreateMultipartUploadArgsEnd(builder)
		builder.Finish(cmaOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpCreateMultipartUpload, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"CreateMultipartUpload should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_UploadPart_DispatchesToBackend verifies UploadPart operation.
	It("uploadpart dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("upload-bucket")
		keyStr := builder.CreateString("upload-key")
		uploadIDStr := builder.CreateString("test-upload-id")
		partData := builder.CreateByteVector([]byte("part-data-body"))

		raftpb.UploadPartArgsStart(builder)
		raftpb.UploadPartArgsAddBucket(builder, bucketStr)
		raftpb.UploadPartArgsAddKey(builder, keyStr)
		raftpb.UploadPartArgsAddUploadId(builder, uploadIDStr)
		raftpb.UploadPartArgsAddPartNumber(builder, 1)
		raftpb.UploadPartArgsAddBody(builder, partData)
		upaOffset := raftpb.UploadPartArgsEnd(builder)
		builder.Finish(upaOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpUploadPart, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"UploadPart should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_CompleteMultipartUpload_DispatchesToBackend verifies CompleteMultipartUpload operation.
	It("completemultipartupload dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("complete-bucket")
		keyStr := builder.CreateString("complete-key")
		uploadIDStr := builder.CreateString("test-upload-id")

		// Build PartRef vector
		etag1Str := builder.CreateString("etag-1")
		raftpb.PartRefStart(builder)
		raftpb.PartRefAddPartNumber(builder, 1)
		raftpb.PartRefAddEtag(builder, etag1Str)
		part1Offset := raftpb.PartRefEnd(builder)

		etag2Str := builder.CreateString("etag-2")
		raftpb.PartRefStart(builder)
		raftpb.PartRefAddPartNumber(builder, 2)
		raftpb.PartRefAddEtag(builder, etag2Str)
		part2Offset := raftpb.PartRefEnd(builder)

		// Create vector
		raftpb.CompleteMultipartUploadArgsStartPartsVector(builder, 2)
		builder.PrependUOffsetT(part2Offset)
		builder.PrependUOffsetT(part1Offset)
		partsVector := builder.EndVector(2)

		raftpb.CompleteMultipartUploadArgsStart(builder)
		raftpb.CompleteMultipartUploadArgsAddBucket(builder, bucketStr)
		raftpb.CompleteMultipartUploadArgsAddKey(builder, keyStr)
		raftpb.CompleteMultipartUploadArgsAddUploadId(builder, uploadIDStr)
		raftpb.CompleteMultipartUploadArgsAddParts(builder, partsVector)
		cmaOffset := raftpb.CompleteMultipartUploadArgsEnd(builder)
		builder.Finish(cmaOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpCompleteMultipartUpload, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"CompleteMultipartUpload should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

	// TestForwardReceiver_AbortMultipartUpload_DispatchesToBackend verifies AbortMultipartUpload operation.
	It("abortmultipartupload dispatchestobackend", func() {
		t := GinkgoT()
		rcv, mgr := setupReceiver(t, "node1")

		mockDist := &DistributedBackend{}
		gb := WrapDistributedBackend("g1", mockDist)
		mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

		builder := flatbuffers.NewBuilder(0)
		bucketStr := builder.CreateString("abort-bucket")
		keyStr := builder.CreateString("abort-key")
		uploadIDStr := builder.CreateString("test-upload-id")

		raftpb.AbortMultipartUploadArgsStart(builder)
		raftpb.AbortMultipartUploadArgsAddBucket(builder, bucketStr)
		raftpb.AbortMultipartUploadArgsAddKey(builder, keyStr)
		raftpb.AbortMultipartUploadArgsAddUploadId(builder, uploadIDStr)
		amaOffset := raftpb.AbortMultipartUploadArgsEnd(builder)
		builder.Finish(amaOffset)

		payload := encodeForwardPayload("g1", raftpb.ForwardOpAbortMultipartUpload, builder.FinishedBytes())
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

		reply := rcv.Handle(msg)
		Expect(reply).NotTo(BeNil())

		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		Expect(fr.Status()).To(Equal(raftpb.ForwardStatusNotLeader),
			"AbortMultipartUpload should decode and attempt dispatch, returning NotLeader for nil RaftNode")
	})

})
