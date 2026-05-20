package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/rs/zerolog/log"
)

// ScrubSessionLookup is the slim contract ForwardReceiver needs to answer
// ForwardOpScrubSessionStat without pulling the whole scrubber.Director
// surface into the cluster package.
type ScrubSessionLookup interface {
	GetSession(id string) (scrubber.Session, bool)
}

type ForwardReceiver struct {
	groups *DataGroupManager
	// scrubLookup is wired via WithScrubSessionLookup; reads happen on every
	// inbound forward and writes only at startup, but we use atomic.Pointer
	// so -race is clean even if rewiring ever lands.
	scrubLookup          atomic.Pointer[ScrubSessionLookup]
	indexProposer        objectIndexProposer // nil = no index commit (single-node / test)
	maxForwardReplyBytes int64               // zero uses DefaultMaxForwardReplyBytes
}

const (
	forwardReadAtBuffer4KiB  = 4 * 1024
	forwardReadAtBuffer16KiB = 16 * 1024
	forwardReadAtBuffer64KiB = 64 * 1024
	forwardReadFenceTimeout  = 5 * time.Second
)

type forwardReadAtBufferPool struct {
	size int
	ch   chan []byte
}

var (
	forwardReadAtBuffer4KiBPool  = newForwardReadAtBufferPool(forwardReadAtBuffer4KiB)
	forwardReadAtBuffer16KiBPool = newForwardReadAtBufferPool(forwardReadAtBuffer16KiB)
	forwardReadAtBuffer64KiBPool = newForwardReadAtBufferPool(forwardReadAtBuffer64KiB)
)

func newForwardReadAtBufferPool(size int) *forwardReadAtBufferPool {
	p := &forwardReadAtBufferPool{
		size: size,
		ch:   make(chan []byte, 8),
	}
	p.ch <- make([]byte, size)
	return p
}

func (p *forwardReadAtBufferPool) Get() []byte {
	select {
	case buf := <-p.ch:
		return buf
	default:
		return make([]byte, p.size)
	}
}

func (p *forwardReadAtBufferPool) Put(buf []byte) {
	select {
	case p.ch <- buf:
	default:
	}
}

func getForwardReadAtBuffer(length int64) ([]byte, int) {
	if length == 0 {
		return nil, 0
	}
	if length <= forwardReadAtBuffer4KiB {
		buf := forwardReadAtBuffer4KiBPool.Get()
		return buf[:int(length)], forwardReadAtBuffer4KiB
	}
	if length <= forwardReadAtBuffer16KiB {
		buf := forwardReadAtBuffer16KiBPool.Get()
		return buf[:int(length)], forwardReadAtBuffer16KiB
	}
	if length <= forwardReadAtBuffer64KiB {
		buf := forwardReadAtBuffer64KiBPool.Get()
		return buf[:int(length)], forwardReadAtBuffer64KiB
	}
	return make([]byte, int(length)), 0
}

func putForwardReadAtBuffer(buf []byte, class int) {
	var p *forwardReadAtBufferPool
	switch class {
	case 0:
		return
	case forwardReadAtBuffer4KiB:
		p = forwardReadAtBuffer4KiBPool
	case forwardReadAtBuffer16KiB:
		p = forwardReadAtBuffer16KiBPool
	case forwardReadAtBuffer64KiB:
		p = forwardReadAtBuffer64KiBPool
	default:
		return
	}
	buf = buf[:class]
	for i := range buf {
		buf[i] = 0
	}
	p.Put(buf)
}

func NewForwardReceiver(groups *DataGroupManager) *ForwardReceiver {
	return &ForwardReceiver{groups: groups}
}

func (r *ForwardReceiver) forwardReplyBytesLimit() int64 {
	if r.maxForwardReplyBytes > 0 {
		return r.maxForwardReplyBytes
	}
	return DefaultMaxForwardReplyBytes
}

// WithScrubSessionLookup wires the per-node Director session lookup so the
// receiver can answer ForwardOpScrubSessionStat. nil = lookup disabled →
// peer scrub-stat queries return found=false.
func (r *ForwardReceiver) WithScrubSessionLookup(lookup ScrubSessionLookup) *ForwardReceiver {
	if lookup == nil {
		r.scrubLookup.Store(nil)
		return r
	}
	r.scrubLookup.Store(&lookup)
	return r
}

// WithObjectIndexProposer wires the object-index proposer so mutating forward
// handlers commit the index entry on the leader side, eliminating the crash
// window between storage write and index commit.
func (r *ForwardReceiver) WithObjectIndexProposer(p objectIndexProposer) *ForwardReceiver {
	r.indexProposer = p
	return r
}

func (r *ForwardReceiver) requireObjectIndexProposer(bucket, key string) (objectIndexProposer, *transport.Message) {
	if r.indexProposer != nil {
		return r.indexProposer, nil
	}
	log.Error().Str("bucket", bucket).Str("key", key).Msg("forward: missing object-index proposer for mutating operation")
	return nil, statusReply(raftpb.ForwardStatusInternal)
}

// Register installs this ForwardReceiver as the handler for StreamProposeGroupForward (0x08) on shardSvc.
// The 0x08 stream type is used for intra-cluster forwarding of bucket-scoped operations.
func (r *ForwardReceiver) Register(shardSvc *ShardService) {
	shardSvc.RegisterHandler(transport.StreamProposeGroupForward, r.Handle)
	shardSvc.RegisterHandler(transport.StreamDataGroupProposeForward, r.HandleGroupPropose)
	shardSvc.RegisterBodyHandler(transport.StreamGroupForwardBody, r.HandleBody)
	shardSvc.RegisterReadHandler(transport.StreamGroupForwardRead, r.HandleRead)
}

// HandleGroupPropose forwards a raw DistributedBackend metadata command to the
// matching local data-group raft node.
func (r *ForwardReceiver) HandleGroupPropose(req *transport.Message) *transport.Message {
	groupID, data, err := decodeGroupForwardPayload(req.Payload)
	if err != nil {
		return groupProposeReply(0, err)
	}
	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil || dg.Backend().Node() == nil {
		return groupProposeReply(0, errors.New("group propose: not voter"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := dg.Backend().Node().ProposeWait(ctx, data)
	if err == nil {
		if waitErr := dg.Backend().WaitApplied(ctx, idx); waitErr != nil {
			err = waitErr
		} else if applyErr := dg.Backend().ApplyError(idx); applyErr != nil {
			err = applyErr
		}
	}
	return groupProposeReply(idx, err)
}

func groupProposeReply(index uint64, err error) *transport.Message {
	// Phase A (Task 16): wire-compatible with decodeProposeForwardReply.
	// GroupBackend.ApplyError harvesting will land alongside AppendObject (Task 18+).
	return &transport.Message{
		Type:    transport.StreamDataGroupProposeForward,
		Payload: encodeProposeForwardReply(index, err),
	}
}

// Handle implements transport.Handler for 0x08 stream.
func (r *ForwardReceiver) Handle(req *transport.Message) *transport.Message {
	groupID, op, fbsArgs, err := decodeForwardPayload(req.Payload)
	if err != nil {
		return errReply(raftpb.ForwardStatusInternal, "")
	}

	// ScrubSessionStat is node-scoped (Director-scoped), not group-scoped.
	// It bypasses the DataGroup lookup + leader gate that the rest of the
	// forwarding ops require.
	if op == raftpb.ForwardOpScrubSessionStat {
		return r.handleScrubSessionStat(fbsArgs)
	}
	if _, ok := lookupBucketForwardOpSpec(op); !ok {
		return errReply(raftpb.ForwardStatusInternal, "")
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		return errReply(raftpb.ForwardStatusNotVoter, "")
	}

	node := dg.Backend().Node()
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
	case raftpb.ForwardOpReadAt:
		return r.handleReadAt(dg, fbsArgs)
	case raftpb.ForwardOpHeadObject:
		return r.handleHeadObject(dg, fbsArgs)
	case raftpb.ForwardOpDeleteObject:
		return r.handleDeleteObject(dg, fbsArgs)
	case raftpb.ForwardOpSetObjectACL:
		return r.handleSetObjectACL(dg, fbsArgs)
	case raftpb.ForwardOpSetObjectTags:
		return r.handleSetObjectTags(dg, fbsArgs)
	case raftpb.ForwardOpGetObjectTags:
		return r.handleGetObjectTags(dg, fbsArgs)
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
	case raftpb.ForwardOpListMultipartUploads:
		return r.handleListMultipartUploads(dg, fbsArgs)
	case raftpb.ForwardOpListParts:
		return r.handleListParts(dg, fbsArgs)
	case raftpb.ForwardOpGetObjectVersion:
		return r.handleGetObjectVersion(dg, fbsArgs)
	case raftpb.ForwardOpDeleteObjectVersion:
		return r.handleDeleteObjectVersion(dg, fbsArgs)
	case raftpb.ForwardOpListObjectVersions:
		return r.handleListObjectVersions(dg, fbsArgs)
	case raftpb.ForwardOpHeadObjectVersion:
		return r.handleHeadObjectVersion(dg, fbsArgs)
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
	spec, ok := lookupBucketForwardOpSpec(op)
	if !ok || !spec.allowedOn(forwardBodyStream) {
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusInternal, "")
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusNotVoter, "")
	}

	node := dg.Backend().Node()
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
	case raftpb.ForwardOpAppendObject:
		return r.handleAppendObjectStream(dg, fbsArgs, body)
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
	spec, ok := lookupBucketForwardOpSpec(op)
	if !ok || !spec.allowedOn(forwardReadStream) {
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		return errReply(raftpb.ForwardStatusNotVoter, ""), nil
	}

	node := dg.Backend().Node()
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
	case raftpb.ForwardOpReadAt:
		return r.handleReadAtRead(dg, fbsArgs)
	default:
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}
}

func drainForwardBody(body io.Reader) {
	if body != nil {
		_, _ = io.Copy(io.Discard, body)
	}
}

func contextForForwardedGroup(ctx context.Context, dg *DataGroup) context.Context {
	if dg == nil {
		return ctx
	}
	return ContextWithPlacementGroupEntry(ctx, ShardGroupEntry{
		ID:      dg.ID(),
		PeerIDs: dg.PeerIDs(),
	})
}

// buildObjectIndexEntry builds an ObjectIndexEntry from a ShardGroupEntry and
// the storage.Object returned by a mutating operation. Shared by
// commitObjectIndex (ClusterCoordinator) and objectIndexEntryForDataGroup
// (ForwardReceiver) to keep the EC config lookup in one place.
func buildObjectIndexEntry(group ShardGroupEntry, bucket, key string, obj *storage.Object, isDeleteMarker bool) ObjectIndexEntry {
	ecCfg := objectIndexECConfigForGroup(group)
	return ObjectIndexEntry{
		Bucket:           bucket,
		Key:              key,
		VersionID:        obj.VersionID,
		PlacementGroupID: group.ID,
		Size:             obj.Size,
		ContentType:      obj.ContentType,
		ETag:             obj.ETag,
		ModTime:          obj.LastModified,
		ECData:           uint8(ecCfg.DataShards),
		ECParity:         uint8(ecCfg.ParityShards),
		NodeIDs:          objectIndexNodeIDsForGroup(group, ecCfg),
		IsDeleteMarker:   isDeleteMarker,
		Parts:            obj.Parts,
	}
}

// objectIndexEntryForDataGroup builds an ObjectIndexEntry from the DataGroup
// topology and the storage.Object returned by a mutating operation.
func objectIndexEntryForDataGroup(dg *DataGroup, bucket, key string, obj *storage.Object, isDeleteMarker bool) ObjectIndexEntry {
	return buildObjectIndexEntry(ShardGroupEntry{ID: dg.ID(), PeerIDs: dg.PeerIDs()}, bucket, key, obj, isDeleteMarker)
}

func (r *ForwardReceiver) handlePutObject(dg *DataGroup, args []byte) *transport.Message {
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	bucket := string(pa.Bucket())
	key := string(pa.Key())
	indexProposer, missingIndexReply := r.requireObjectIndexProposer(bucket, key)
	if missingIndexReply != nil {
		return missingIndexReply
	}
	ctx := ContextWithPutTrace(contextForForwardedGroup(context.Background(), dg), PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		GroupID:     dg.ID(),
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   putTraceSizeClass(int64(len(pa.BodyBytes())), DefaultMaxForwardBodyBytes),
		ForwardMode: PutTraceForwardFrame,
	})
	ObservePutTraceStage(ctx, PutTraceStageForwardReceiverDispatch, time.Now(), PutTraceStageFields{})
	stageStart := time.Now()
	obj, err := dg.Backend().PutObject(ctx, bucket, key, bytes.NewReader(pa.BodyBytes()), string(pa.ContentType()))
	fields := PutTraceStageFields{Bytes: int64(len(pa.BodyBytes()))}
	if err != nil {
		fields.Error = err.Error()
		ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
		return statusReply(mapErrorToStatus(err))
	}
	ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
	entry := objectIndexEntryForDataGroup(dg, bucket, key, obj, false)
	stageStart = time.Now()
	err = indexProposer.ProposeObjectIndex(ctx, entry, false)
	fields = PutTraceStageFields{MetaProposeSite: "receiver", MetaProposeCount: 1}
	if err != nil {
		fields.Error = err.Error()
		ObservePutTraceStage(ctx, PutTraceStageMetaIndexPropose, stageStart, fields)
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("forward: ProposeObjectIndex failed; orphan may be created")
		return statusReply(raftpb.ForwardStatusInternal)
	}
	ObservePutTraceStage(ctx, PutTraceStageMetaIndexPropose, stageStart, fields)
	return &transport.Message{Payload: buildObjectReply(obj, bucket)}
}

func (r *ForwardReceiver) handlePutObjectStream(dg *DataGroup, args []byte, body io.Reader) *transport.Message {
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	bucket := string(pa.Bucket())
	key := string(pa.Key())
	indexProposer, missingIndexReply := r.requireObjectIndexProposer(bucket, key)
	if missingIndexReply != nil {
		drainForwardBody(body)
		return missingIndexReply
	}
	ctx := ContextWithPutTrace(contextForForwardedGroup(context.Background(), dg), PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		GroupID:     dg.ID(),
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   PutTraceSizeLarge,
		ForwardMode: PutTraceForwardStream,
	})
	ObservePutTraceStage(ctx, PutTraceStageForwardReceiverDispatch, time.Now(), PutTraceStageFields{})
	stageStart := time.Now()
	obj, err := dg.Backend().PutObject(ctx, bucket, key, body, string(pa.ContentType()))
	fields := PutTraceStageFields{}
	if err != nil {
		fields.Error = err.Error()
		ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
		return statusReply(mapErrorToStatus(err))
	}
	ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
	entry := objectIndexEntryForDataGroup(dg, bucket, key, obj, false)
	stageStart = time.Now()
	err = indexProposer.ProposeObjectIndex(ctx, entry, false)
	fields = PutTraceStageFields{MetaProposeSite: "receiver", MetaProposeCount: 1}
	if err != nil {
		fields.Error = err.Error()
		ObservePutTraceStage(ctx, PutTraceStageMetaIndexPropose, stageStart, fields)
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("forward: ProposeObjectIndex failed; orphan may be created")
		return statusReply(raftpb.ForwardStatusInternal)
	}
	ObservePutTraceStage(ctx, PutTraceStageMetaIndexPropose, stageStart, fields)
	return &transport.Message{Payload: buildObjectReply(obj, bucket)}
}

func (r *ForwardReceiver) handleGetObjectRead(dg *DataGroup, args []byte) (*transport.Message, io.ReadCloser) {
	ctx := context.Background()
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	rc, obj, err := dg.Backend().GetObject(ctx, string(ga.Bucket()), string(ga.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	return &transport.Message{Payload: buildGetObjectReply(obj, string(ga.Bucket()), nil)}, rc
}

func (r *ForwardReceiver) handleReadAtRead(dg *DataGroup, args []byte) (*transport.Message, io.ReadCloser) {
	ra := raftpb.GetRootAsReadAtArgs(args, 0)
	length := ra.Length()
	if ra.Offset() < 0 || length < 0 {
		return statusReply(raftpb.ForwardStatusInternal), nil
	}
	body := &backendReadAtStream{
		ctx:     context.Background(),
		backend: dg.Backend(),
		bucket:  string(ra.Bucket()),
		key:     string(ra.Key()),
		offset:  ra.Offset(),
		length:  length,
	}
	return &transport.Message{Payload: buildOKReply()}, body
}

func (r *ForwardReceiver) handleReadAt(dg *DataGroup, args []byte) *transport.Message {
	ra := raftpb.GetRootAsReadAtArgs(args, 0)
	length := ra.Length()
	if ra.Offset() < 0 || length < 0 || length > DefaultMaxForwardReplyBytes {
		return statusReply(raftpb.ForwardStatusInternal)
	}
	buf, pooled := getForwardReadAtBuffer(length)
	defer putForwardReadAtBuffer(buf, pooled)
	n, err := dg.Backend().ReadAt(context.Background(), string(ra.Bucket()), string(ra.Key()), ra.Offset(), buf)
	if err != nil && !(errors.Is(err, io.EOF) && n > 0) {
		return statusReply(mapErrorToStatus(err))
	}
	if n < 0 || n > len(buf) {
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildReadAtReply(buf[:n])}
}

type backendReadAtStream struct {
	ctx            context.Context
	backend        *GroupBackend
	bucket, key    string
	offset, length int64
	pos            int64
}

func (r *backendReadAtStream) Read(p []byte) (int, error) {
	if r.pos >= r.length {
		return 0, io.EOF
	}
	if remaining := r.length - r.pos; int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.backend.ReadAt(r.ctx, r.bucket, r.key, r.offset+r.pos, p)
	r.pos += int64(n)
	if err != nil && n > 0 && r.pos >= r.length {
		return n, nil
	}
	return n, err
}

func (r *backendReadAtStream) Close() error { return nil }

func (r *ForwardReceiver) handleGetObject(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	rc, obj, err := dg.Backend().GetObject(ctx, string(ga.Bucket()), string(ga.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	defer rc.Close()
	replyLimit := r.forwardReplyBytesLimit()
	body, err := io.ReadAll(io.LimitReader(rc, replyLimit+1))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	if int64(len(body)) > replyLimit {
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
	replyLimit := r.forwardReplyBytesLimit()
	body, err := io.ReadAll(io.LimitReader(rc, replyLimit+1))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	if int64(len(body)) > replyLimit {
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
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	obj, err := dg.Backend().HeadObject(ctx, string(ha.Bucket()), string(ha.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, string(ha.Bucket()))}
}

func waitForwardReadFence(ctx context.Context, gb *GroupBackend) error {
	if gb == nil {
		return ErrNoGroup
	}
	readCtx, cancel := context.WithTimeout(ctx, forwardReadFenceTimeout)
	defer cancel()
	idx, err := gb.ReadIndex(readCtx)
	if err != nil {
		return err
	}
	return gb.WaitApplied(readCtx, idx)
}

func (r *ForwardReceiver) handleHeadObjectVersion(dg *DataGroup, args []byte) *transport.Message {
	ha := raftpb.GetRootAsHeadObjectVersionArgs(args, 0)
	bucket := string(ha.Bucket())
	obj, err := dg.Backend().HeadObjectVersion(bucket, string(ha.Key()), string(ha.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectReply(obj, bucket)}
}

func (r *ForwardReceiver) handleDeleteObject(dg *DataGroup, args []byte) *transport.Message {
	da := raftpb.GetRootAsDeleteObjectArgs(args, 0)
	bucket := string(da.Bucket())
	key := string(da.Key())
	indexProposer, missingIndexReply := r.requireObjectIndexProposer(bucket, key)
	if missingIndexReply != nil {
		return missingIndexReply
	}
	markerID, err := dg.Backend().DeleteObjectReturningMarker(bucket, key)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	ctx := contextForForwardedGroup(context.Background(), dg)
	marker := &storage.Object{Key: key, VersionID: markerID}
	entry := objectIndexEntryForDataGroup(dg, bucket, key, marker, true)
	if err := indexProposer.ProposeObjectIndex(ctx, entry, false); err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("forward: ProposeObjectIndex (delete marker) failed; orphan may be created")
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildObjectReply(&storage.Object{
		Key:       key,
		VersionID: markerID,
	}, bucket)}
}

func (r *ForwardReceiver) handleSetObjectACL(dg *DataGroup, args []byte) *transport.Message {
	sa := raftpb.GetRootAsSetObjectACLArgs(args, 0)
	if err := dg.Backend().SetObjectACL(string(sa.Bucket()), string(sa.Key()), sa.Acl()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildOKReply()}
}

func (r *ForwardReceiver) handleSetObjectTags(dg *DataGroup, args []byte) *transport.Message {
	sa := raftpb.GetRootAsSetObjectTagsArgs(args, 0)

	tags := make([]storage.Tag, 0, sa.TagsLength())
	for i := 0; i < sa.TagsLength(); i++ {
		var t raftpb.Tag
		if sa.Tags(&t, i) {
			tags = append(tags, storage.Tag{
				Key:   string(t.Key()),
				Value: string(t.Value()),
			})
		}
	}

	if err := dg.Backend().SetObjectTags(string(sa.Bucket()), string(sa.Key()), string(sa.VersionId()), tags); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildOKReply()}
}

func (r *ForwardReceiver) handleGetObjectTags(dg *DataGroup, args []byte) *transport.Message {
	ga := raftpb.GetRootAsGetObjectTagsArgs(args, 0)
	tags, err := dg.Backend().GetObjectTags(string(ga.Bucket()), string(ga.Key()), string(ga.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildGetObjectTagsReply(tags)}
}

func (r *ForwardReceiver) handleDeleteObjectVersion(dg *DataGroup, args []byte) *transport.Message {
	da := raftpb.GetRootAsDeleteObjectVersionArgs(args, 0)
	bucket := string(da.Bucket())
	key := string(da.Key())
	versionID := string(da.VersionId())
	indexProposer, missingIndexReply := r.requireObjectIndexProposer(bucket, key)
	if missingIndexReply != nil {
		return missingIndexReply
	}
	if err := dg.Backend().DeleteObjectVersion(bucket, key, versionID); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	ctx := contextForForwardedGroup(context.Background(), dg)
	if err := indexProposer.ProposeDeleteObjectIndex(ctx, bucket, key, versionID); err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Str("version_id", versionID).Msg("forward: ProposeDeleteObjectIndex failed; stale index entry may remain")
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildOKReply()}
}

func (r *ForwardReceiver) handleListObjects(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	la := raftpb.GetRootAsListObjectsArgs(args, 0)
	bucket := string(la.Bucket())
	prefix := string(la.Prefix())
	marker := string(la.Marker())
	maxKeys := int(la.MaxKeys())
	// dg.Backend() is *GroupBackend, which embeds *DistributedBackend and
	// inherits ListObjectsPage. Honoring marker here keeps forwarded reads
	// paginating correctly past the first window — pre-fix this dropped
	// every page beyond marker silently because args carried no marker.
	//
	// The forward reply has no dedicated `truncated` slot, so we encode it
	// in the length: request maxKeys+1 entries and let the coordinator
	// detect truncation via `len(objs) > maxKeys`. Without the +1 the
	// receiver caps at maxKeys, the coordinator sees ≤maxKeys, and
	// IsTruncated is permanently false on cross-group reads.
	probeMax := maxKeys
	if probeMax > 0 {
		probeMax = maxKeys + 1
	}
	objs, _, err := dg.Backend().ListObjectsPage(ctx, bucket, prefix, marker, probeMax)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildObjectsReply(bucket, objs)}
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
	bucket := string(ca.Bucket())
	key := string(ca.Key())
	contentType := string(ca.ContentType())

	// Tags vector absent (older senders) or empty: route to the no-tags
	// overload to preserve wire compatibility. Non-empty: dispatch to the
	// tags-carrying overload so tags reach clusterMultipartMeta on the
	// resolved data group.
	if n := ca.TagsLength(); n > 0 {
		tags := make([]storage.Tag, 0, n)
		for i := 0; i < n; i++ {
			var t raftpb.Tag
			if ca.Tags(&t, i) {
				tags = append(tags, storage.Tag{
					Key:   string(t.Key()),
					Value: string(t.Value()),
				})
			}
		}
		uploadID, err := dg.Backend().CreateMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
		if err != nil {
			return statusReply(mapErrorToStatus(err))
		}
		return &transport.Message{Payload: buildUploadReply(bucket, key, uploadID)}
	}

	upload, err := dg.Backend().CreateMultipartUpload(ctx, bucket, key, contentType)
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

// handleAppendObjectStream dispatches a forwarded AppendObject. Body bytes are
// streamed on the same QUIC stream and consumed by the owner-side
// DistributedBackend.AppendObject directly (no buffering on the receiver).
// After successful apply we commit the ObjectIndex entry so the meta-Raft view
// reflects the new size/etag — mirrors handlePutObject's commit pattern.
func (r *ForwardReceiver) handleAppendObjectStream(dg *DataGroup, args []byte, body io.Reader) *transport.Message {
	aa := raftpb.GetRootAsAppendObjectForwardArgs(args, 0)
	bucket := string(aa.Bucket())
	key := string(aa.Key())
	indexProposer, missingIndexReply := r.requireObjectIndexProposer(bucket, key)
	if missingIndexReply != nil {
		drainForwardBody(body)
		return missingIndexReply
	}
	ctx := contextForForwardedGroup(context.Background(), dg)
	obj, err := dg.Backend().AppendObject(ctx, bucket, key, aa.ExpectedOffset(), body)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	entry := objectIndexEntryForDataGroup(dg, bucket, key, obj, false)
	if err := indexProposer.ProposeObjectIndex(ctx, entry, false); err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("forward: ProposeObjectIndex failed after AppendObject; orphan may be created")
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildObjectReply(obj, bucket)}
}

func (r *ForwardReceiver) handleCompleteMultipartUpload(dg *DataGroup, args []byte) *transport.Message {
	ctx := contextForForwardedGroup(context.Background(), dg)
	ca := raftpb.GetRootAsCompleteMultipartUploadArgs(args, 0)
	bucket := string(ca.Bucket())
	key := string(ca.Key())
	indexProposer, missingIndexReply := r.requireObjectIndexProposer(bucket, key)
	if missingIndexReply != nil {
		return missingIndexReply
	}
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
	obj, err := dg.Backend().CompleteMultipartUpload(ctx, bucket, key, string(ca.UploadId()), parts)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	entry := objectIndexEntryForDataGroup(dg, bucket, key, obj, false)
	if err := indexProposer.ProposeObjectIndex(ctx, entry, false); err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("forward: ProposeObjectIndex failed; orphan may be created")
		return statusReply(raftpb.ForwardStatusInternal)
	}
	return &transport.Message{Payload: buildObjectReply(obj, bucket)}
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

func (r *ForwardReceiver) handleListMultipartUploads(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	la := raftpb.GetRootAsListMultipartUploadsArgs(args, 0)
	uploads, err := dg.Backend().ListMultipartUploads(
		ctx,
		string(la.Bucket()),
		string(la.Prefix()),
		int(la.MaxUploads()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildMultipartUploadsReply(uploads)}
}

func (r *ForwardReceiver) handleListParts(dg *DataGroup, args []byte) *transport.Message {
	ctx := context.Background()
	la := raftpb.GetRootAsListPartsArgs(args, 0)
	parts, err := dg.Backend().ListParts(
		ctx,
		string(la.Bucket()),
		string(la.Key()),
		string(la.UploadId()),
		int(la.MaxParts()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return &transport.Message{Payload: buildPartsReply(parts)}
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
	if errors.Is(err, storage.ErrMethodNotAllowed) {
		return raftpb.ForwardStatusMethodNotAllowed
	}
	if errors.Is(err, storage.ErrUploadNotFound) {
		return raftpb.ForwardStatusNoSuchUpload
	}
	if errors.Is(err, storage.ErrEntityTooLarge) {
		return raftpb.ForwardStatusEntityTooLarge
	}
	if errors.Is(err, storage.ErrAppendOffsetMismatch) {
		return raftpb.ForwardStatusAppendOffsetMismatch
	}
	if errors.Is(err, storage.ErrAppendNotSupported) {
		return raftpb.ForwardStatusAppendNotSupported
	}
	if errors.Is(err, storage.ErrAppendCapExceeded) {
		return raftpb.ForwardStatusAppendCapExceeded
	}
	if errors.Is(err, storage.ErrAppendObjectTooLarge) {
		return raftpb.ForwardStatusAppendObjectTooLarge
	}
	if errors.Is(err, ErrPlacementTargetsUnavailable) {
		return raftpb.ForwardStatusInsufficientPlacementTargets
	}
	log.Warn().Err(err).Msg("forward receiver mapped backend error to internal status")
	return raftpb.ForwardStatusInternal
}

func (r *ForwardReceiver) handleScrubSessionStat(fbsArgs []byte) *transport.Message {
	if len(fbsArgs) == 0 {
		return errReply(raftpb.ForwardStatusInternal, "")
	}
	args := raftpb.GetRootAsScrubSessionStatArgs(fbsArgs, 0)
	sessionID := string(args.SessionId())
	var sess scrubber.Session
	found := false
	if lookupPtr := r.scrubLookup.Load(); lookupPtr != nil && sessionID != "" {
		sess, found = (*lookupPtr).GetSession(sessionID)
	}
	return buildScrubSessionStatReply(found, sess)
}

func buildScrubSessionStatReply(found bool, sess scrubber.Session) *transport.Message {
	b := flatbuffers.NewBuilder(256)
	bktOff := b.CreateString(sess.Bucket)
	pfxOff := b.CreateString(sess.KeyPrefix)
	statusOff := b.CreateString(sess.Status)

	raftpb.ScrubSessionStatReplyStart(b)
	raftpb.ScrubSessionStatReplyAddFound(b, found)
	raftpb.ScrubSessionStatReplyAddBucket(b, bktOff)
	raftpb.ScrubSessionStatReplyAddKeyPrefix(b, pfxOff)
	raftpb.ScrubSessionStatReplyAddScope(b, int32(sess.Scope))
	raftpb.ScrubSessionStatReplyAddDryRun(b, sess.DryRun)
	raftpb.ScrubSessionStatReplyAddStatus(b, statusOff)
	raftpb.ScrubSessionStatReplyAddStartedAt(b, sess.StartedAt.Unix())
	if !sess.DoneAt.IsZero() {
		raftpb.ScrubSessionStatReplyAddDoneAt(b, sess.DoneAt.Unix())
	}
	raftpb.ScrubSessionStatReplyAddChecked(b, sess.Stats.Checked)
	raftpb.ScrubSessionStatReplyAddHealthy(b, sess.Stats.Healthy)
	raftpb.ScrubSessionStatReplyAddDetected(b, sess.Stats.Detected)
	raftpb.ScrubSessionStatReplyAddRepaired(b, sess.Stats.Repaired)
	raftpb.ScrubSessionStatReplyAddUnrepairable(b, sess.Stats.Unrepairable)
	raftpb.ScrubSessionStatReplyAddSkipped(b, sess.Stats.Skipped)
	raftpb.ScrubSessionStatReplyAddOwnedHere(b, found && sess.Stats.Checked > 0)
	scrubReplyOff := raftpb.ScrubSessionStatReplyEnd(b)

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddScrubSession(b, scrubReplyOff)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return &transport.Message{Type: transport.StreamProposeGroupForward, Payload: b.FinishedBytes()}
}
