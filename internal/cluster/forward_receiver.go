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
	maxForwardReplyBytes int64 // zero uses DefaultMaxForwardReplyBytes
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

// Register installs this ForwardReceiver's buffered-route handlers on shardSvc's
// transport: /forward/propose/group (bucket-scoped operation forwarding) and
// /forward/propose/data-group (metadata proposal forwarding). Every forward
// outcome (NotVoter/NotLeader+hint/OK/...) is in-band in the FB reply.
func (r *ForwardReceiver) Register(shardSvc *ShardService) {
	shardSvc.RegisterBufferedRoute(transport.RouteForwardProposeGroup, r.Handle)
	shardSvc.RegisterBufferedRoute(transport.RouteForwardProposeDataGroup, r.HandleGroupPropose)
}

// HandleGroupPropose forwards a raw DistributedBackend metadata command to the
// matching local data-group raft node. The propose outcome (index + apply
// error) is in-band via encodeProposeForwardReply.
func (r *ForwardReceiver) HandleGroupPropose(payload []byte) ([]byte, error) {
	groupID, data, err := decodeGroupForwardPayload(payload)
	if err != nil {
		return groupProposeReply(0, err)
	}
	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil || dg.Backend().Node() == nil {
		return groupProposeReply(0, errors.New("group propose: not voter"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), proposeForwardTimeout)
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

func groupProposeReply(index uint64, err error) ([]byte, error) {
	// Phase A (Task 16): wire-compatible with decodeProposeForwardReply.
	// GroupBackend.ApplyError harvesting will land alongside AppendObject (Task 18+).
	return encodeProposeForwardReply(index, err), nil
}

// Handle serves one buffered /forward/propose/group request. Every outcome is
// in-band in the FB ForwardReply (the returned error is always nil).
func (r *ForwardReceiver) Handle(payload []byte) ([]byte, error) {
	groupID, op, fbsArgs, err := decodeForwardPayload(payload)
	if err != nil {
		log.Debug().Err(err).Msg("forward: decode payload failed")
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}
	log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward: receive")

	// ScrubSessionStat is node-scoped (Director-scoped), not group-scoped.
	// It bypasses the DataGroup lookup + leader gate that the rest of the
	// forwarding ops require.
	if op == raftpb.ForwardOpScrubSessionStat {
		return r.handleScrubSessionStat(fbsArgs), nil
	}
	spec, ok := lookupBucketForwardOpSpec(op)
	if !ok {
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward: not voter")
		return errReply(raftpb.ForwardStatusNotVoter, ""), nil
	}

	node := dg.Backend().Node()
	if node == nil || !node.IsLeader() {
		hint := ""
		if node != nil {
			hint = node.LeaderID()
		}
		log.Debug().Str("group_id", groupID).Str("op", op.String()).Str("leader_hint", hint).Msg("forward: not leader")
		return errReply(raftpb.ForwardStatusNotLeader, hint), nil
	}
	log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward: dispatch leader")

	if spec.handleFrame == nil {
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}
	return spec.handleFrame(r, dg, fbsArgs), nil
}

// HandleBody implements streamed-body forwarding for PutObject and UploadPart
// (the native /forward/write route, transport.RegisterForwardWriteHandler).
// The frame carries group/op/metadata; body is the raw request stream, passed
// directly into the local GroupBackend. Every outcome is in-band in the FB
// ForwardReply (the returned error is always nil).
func (r *ForwardReceiver) HandleBody(frame []byte, body io.Reader) ([]byte, error) {
	groupID, op, fbsArgs, err := decodeForwardPayload(frame)
	if err != nil {
		log.Debug().Err(err).Msg("forward body: decode payload failed")
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}
	log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward body: receive")
	spec, ok := lookupBucketForwardOpSpec(op)
	if !ok || !spec.allowedOn(forwardBodyStream) {
		log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward body: unsupported op")
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward body: not voter")
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusNotVoter, ""), nil
	}

	node := dg.Backend().Node()
	if node == nil || !node.IsLeader() {
		drainForwardBody(body)
		hint := ""
		if node != nil {
			hint = node.LeaderID()
		}
		log.Debug().Str("group_id", groupID).Str("op", op.String()).Str("leader_hint", hint).Msg("forward body: not leader")
		return errReply(raftpb.ForwardStatusNotLeader, hint), nil
	}
	log.Debug().Str("group_id", groupID).Str("op", op.String()).Msg("forward body: dispatch leader")

	if spec.handleBody == nil {
		drainForwardBody(body)
		return errReply(raftpb.ForwardStatusInternal, ""), nil
	}
	return spec.handleBody(r, dg, fbsArgs, body), nil
}

// HandleRead implements streamed-response forwarding for GetObject and
// GetObjectVersion (the native /forward/read route,
// transport.RegisterForwardReadHandler). The returned ForwardReply carries
// metadata only; object bytes follow as the streamed response body.
func (r *ForwardReceiver) HandleRead(frame []byte) ([]byte, io.ReadCloser, error) {
	groupID, op, fbsArgs, err := decodeForwardPayload(frame)
	if err != nil {
		return errReply(raftpb.ForwardStatusInternal, ""), nil, nil
	}
	spec, ok := lookupBucketForwardOpSpec(op)
	if !ok || !spec.allowedOn(forwardReadStream) {
		return errReply(raftpb.ForwardStatusInternal, ""), nil, nil
	}

	dg := r.groups.Get(groupID)
	if dg == nil || dg.Backend() == nil {
		return errReply(raftpb.ForwardStatusNotVoter, ""), nil, nil
	}

	node := dg.Backend().Node()
	if node == nil || !node.IsLeader() {
		hint := ""
		if node != nil {
			hint = node.LeaderID()
		}
		return errReply(raftpb.ForwardStatusNotLeader, hint), nil, nil
	}

	if spec.handleRead == nil {
		return errReply(raftpb.ForwardStatusInternal, ""), nil, nil
	}
	reply, rbody := spec.handleRead(r, dg, fbsArgs)
	return reply, rbody, nil
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

// aclPtr maps a wire ACL bitmask to *uint8: a 0 wire value means absent
// (private default) → nil, matching a no-ACL local PUT; a non-zero value →
// &v so the local PUT path persists it.
func aclPtr(v uint8) *uint8 {
	if v == 0 {
		return nil
	}
	return &v
}

func (r *ForwardReceiver) handlePutObject(dg *DataGroup, args []byte) []byte {
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	bucket := string(pa.Bucket())
	key := string(pa.Key())
	ctx := ContextWithPutTrace(contextForForwardedGroup(context.Background(), dg), PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		GroupID:     dg.ID(),
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   putTraceSizeClass(int64(len(pa.BodyBytes())), DefaultMaxForwardBodyBytes),
		ForwardMode: PutTraceForwardFrame,
	})
	ctx = contextWithVersioningState(ctx, pa.VersioningState())
	ctx = contextWithSoleAuthEpochWire(ctx, pa.SoleauthEpoch())
	ObservePutTraceStage(ctx, PutTraceStageForwardReceiverDispatch, time.Now(), PutTraceStageFields{})
	stageStart := time.Now()
	obj, err := dg.Backend().PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:         bucket,
		Key:            key,
		Body:           bytes.NewReader(pa.BodyBytes()),
		ContentType:    string(pa.ContentType()),
		SystemMetadata: storage.ObjectSystemMetadata{SSEAlgorithm: string(pa.SseAlgorithm())},
		UserMetadata:   decodePutObjectUserMetadata(pa),
		ContentMD5Hex:  string(pa.ContentMd5Hex()),
		ACL:            aclPtr(pa.Acl()),
	})
	fields := PutTraceStageFields{Bytes: int64(len(pa.BodyBytes()))}
	if err != nil {
		fields.Error = err.Error()
		ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
		return statusReply(mapErrorToStatus(err))
	}
	ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
	return buildObjectReply(obj, bucket)
}

func (r *ForwardReceiver) handlePutObjectStream(dg *DataGroup, args []byte, body io.Reader) []byte {
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	bucket := string(pa.Bucket())
	key := string(pa.Key())
	ctx := ContextWithPutTrace(contextForForwardedGroup(context.Background(), dg), PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		GroupID:     dg.ID(),
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   PutTraceSizeLarge,
		ForwardMode: PutTraceForwardStream,
	})
	ctx = contextWithVersioningState(ctx, pa.VersioningState())
	ctx = contextWithSoleAuthEpochWire(ctx, pa.SoleauthEpoch())
	ObservePutTraceStage(ctx, PutTraceStageForwardReceiverDispatch, time.Now(), PutTraceStageFields{})
	stageStart := time.Now()
	obj, err := dg.Backend().PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:         bucket,
		Key:            key,
		Body:           body,
		ContentType:    string(pa.ContentType()),
		SystemMetadata: storage.ObjectSystemMetadata{SSEAlgorithm: string(pa.SseAlgorithm())},
		UserMetadata:   decodePutObjectUserMetadata(pa),
		ContentMD5Hex:  string(pa.ContentMd5Hex()),
		ACL:            aclPtr(pa.Acl()),
	})
	fields := PutTraceStageFields{}
	if err != nil {
		fields.Error = err.Error()
		ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
		return statusReply(mapErrorToStatus(err))
	}
	ObservePutTraceStage(ctx, PutTraceStageReceiverBackendPut, stageStart, fields)
	return buildObjectReply(obj, bucket)
}

func (r *ForwardReceiver) handleGetObjectRead(dg *DataGroup, args []byte) ([]byte, io.ReadCloser) {
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), ga.VersioningState())
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	rc, obj, err := dg.Backend().GetObject(ctx, string(ga.Bucket()), string(ga.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	return buildGetObjectReply(obj, string(ga.Bucket()), nil), rc
}

func (r *ForwardReceiver) handleReadAtRead(dg *DataGroup, args []byte) ([]byte, io.ReadCloser) {
	ra := raftpb.GetRootAsReadAtArgs(args, 0)
	length := ra.Length()
	if ra.Offset() < 0 || length < 0 {
		return statusReply(raftpb.ForwardStatusInternal), nil
	}
	// Fence BEFORE returning the stream so every later ReadAt resolution
	// (backendReadAtStream.Read → ReadAt → headObjectMeta) is past the
	// linearizable barrier and cannot read a stale local soleauth state.
	if err := waitForwardReadFence(context.Background(), dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	body := &backendReadAtStream{
		ctx:     context.Background(),
		backend: dg.Backend(),
		bucket:  string(ra.Bucket()),
		key:     string(ra.Key()),
		offset:  ra.Offset(),
		length:  length,
	}
	return buildOKReply(), body
}

func (r *ForwardReceiver) handleReadAt(dg *DataGroup, args []byte) []byte {
	ra := raftpb.GetRootAsReadAtArgs(args, 0)
	length := ra.Length()
	if ra.Offset() < 0 || length < 0 || length > DefaultMaxForwardReplyBytes {
		return statusReply(raftpb.ForwardStatusInternal)
	}
	if err := waitForwardReadFence(context.Background(), dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
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
	return buildReadAtReply(buf[:n])
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

func (r *ForwardReceiver) handleGetObject(dg *DataGroup, args []byte) []byte {
	ga := raftpb.GetRootAsGetObjectArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), ga.VersioningState())
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
	return buildGetObjectReply(obj, string(ga.Bucket()), body)
}

func (r *ForwardReceiver) handleGetObjectVersionRead(dg *DataGroup, args []byte) ([]byte, io.ReadCloser) {
	ga := raftpb.GetRootAsGetObjectVersionArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), ga.VersioningState())
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	rc, obj, err := dg.Backend().getObjectVersionCtx(ctx, string(ga.Bucket()), string(ga.Key()), string(ga.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err)), nil
	}
	return buildGetObjectReply(obj, string(ga.Bucket()), nil), rc
}

func (r *ForwardReceiver) handleGetObjectVersion(dg *DataGroup, args []byte) []byte {
	ga := raftpb.GetRootAsGetObjectVersionArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), ga.VersioningState())
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	rc, obj, err := dg.Backend().getObjectVersionCtx(ctx, string(ga.Bucket()), string(ga.Key()), string(ga.VersionId()))
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
	return buildGetObjectReply(obj, string(ga.Bucket()), body)
}

func (r *ForwardReceiver) handleHeadObject(dg *DataGroup, args []byte) []byte {
	ha := raftpb.GetRootAsHeadObjectArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), ha.VersioningState())
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	obj, err := dg.Backend().HeadObject(ctx, string(ha.Bucket()), string(ha.Key()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildObjectReply(obj, string(ha.Bucket()))
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

func (r *ForwardReceiver) handleHeadObjectVersion(dg *DataGroup, args []byte) []byte {
	ha := raftpb.GetRootAsHeadObjectVersionArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), ha.VersioningState())
	// Read-fence (mirrors handleHeadObject): a lagging forwarded receiver must
	// apply all committed writes before resolving — else it reads a stale local
	// soleauth state and takes the wrong read1 authority branch.
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	bucket := string(ha.Bucket())
	obj, err := dg.Backend().headObjectVersionCtx(ctx, bucket, string(ha.Key()), string(ha.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildObjectReply(obj, bucket)
}

func (r *ForwardReceiver) handleDeleteObject(dg *DataGroup, args []byte) []byte {
	da := raftpb.GetRootAsDeleteObjectArgs(args, 0)
	bucket := string(da.Bucket())
	key := string(da.Key())
	markerID, err := dg.Backend().DeleteObjectReturningMarker(bucket, key)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildObjectReply(&storage.Object{
		Key:       key,
		VersionID: markerID,
	}, bucket)
}

// handleHardDeleteObject serves the soleauth=on force-delete hard-delete of a
// legacy-bare obj:{bucket}/{key} record (CmdDeleteObject VID="", NO tombstone) —
// distinct from handleDeleteObject which writes a delete-marker tombstone.
// Idempotent: a no-op when the bare record is absent on this group.
func (r *ForwardReceiver) handleHardDeleteObject(dg *DataGroup, args []byte) []byte {
	da := raftpb.GetRootAsDeleteObjectArgs(args, 0)
	if err := dg.Backend().HardDeleteLegacyObject(context.Background(), string(da.Bucket()), string(da.Key())); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildOKReply()
}

func (r *ForwardReceiver) handleSetObjectACL(dg *DataGroup, args []byte) []byte {
	sa := raftpb.GetRootAsSetObjectACLArgs(args, 0)
	if err := dg.Backend().SetObjectACL(string(sa.Bucket()), string(sa.Key()), sa.Acl()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildOKReply()
}

func (r *ForwardReceiver) handleSetObjectTags(dg *DataGroup, args []byte) []byte {
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
	return buildOKReply()
}

func (r *ForwardReceiver) handleGetObjectTags(dg *DataGroup, args []byte) []byte {
	ga := raftpb.GetRootAsGetObjectTagsArgs(args, 0)
	if err := waitForwardReadFence(context.Background(), dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	tags, err := dg.Backend().GetObjectTags(string(ga.Bucket()), string(ga.Key()), string(ga.VersionId()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildGetObjectTagsReply(tags)
}

func (r *ForwardReceiver) handleDeleteObjectVersion(dg *DataGroup, args []byte) []byte {
	da := raftpb.GetRootAsDeleteObjectVersionArgs(args, 0)
	bucket := string(da.Bucket())
	key := string(da.Key())
	versionID := string(da.VersionId())
	if err := dg.Backend().DeleteObjectVersion(bucket, key, versionID); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildOKReply()
}

func (r *ForwardReceiver) handleListObjects(dg *DataGroup, args []byte) []byte {
	la := raftpb.GetRootAsListObjectsArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), la.VersioningState())
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
	return buildObjectsReply(bucket, objs)
}

func (r *ForwardReceiver) handleListObjectVersions(dg *DataGroup, args []byte) []byte {
	la := raftpb.GetRootAsListObjectVersionsArgs(args, 0)
	// Read fence + re-stamp the edge versioning decision so a forwarded
	// enumeration carries the same authoritative flag the originating node had.
	ctx := contextWithVersioningState(context.Background(), la.VersioningState())
	if err := waitForwardReadFence(ctx, dg.Backend()); err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	versions, err := dg.Backend().ListObjectVersions(ctx, string(la.Bucket()), string(la.Prefix()), int(la.MaxKeys()))
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildObjectVersionsReply(versions)
}

func (r *ForwardReceiver) handleWalkObjects(dg *DataGroup, args []byte) []byte {
	wa := raftpb.GetRootAsWalkObjectsArgs(args, 0)
	ctx := contextWithVersioningState(context.Background(), wa.VersioningState())
	var objs []*storage.Object
	err := dg.Backend().WalkObjects(ctx, string(wa.Bucket()), string(wa.Prefix()), func(o *storage.Object) error {
		objs = append(objs, o)
		return nil
	})
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildObjectsReply(string(wa.Bucket()), objs)
}

func (r *ForwardReceiver) handleCreateMultipartUpload(dg *DataGroup, args []byte) []byte {
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
		return buildUploadReply(bucket, key, uploadID)
	}

	upload, err := dg.Backend().CreateMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildUploadReply(upload.Bucket, upload.Key, upload.UploadID)
}

func (r *ForwardReceiver) handleUploadPart(dg *DataGroup, args []byte) []byte {
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
		string(ua.ContentMd5Hex()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildPartReply(part)
}

func (r *ForwardReceiver) handleUploadPartStream(dg *DataGroup, args []byte, body io.Reader) []byte {
	ctx := context.Background()
	ua := raftpb.GetRootAsUploadPartArgs(args, 0)
	part, err := dg.Backend().UploadPart(
		ctx,
		string(ua.Bucket()),
		string(ua.Key()),
		string(ua.UploadId()),
		int(ua.PartNumber()),
		body,
		string(ua.ContentMd5Hex()),
	)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildPartReply(part)
}

// handleAppendObjectStream dispatches a forwarded AppendObject. Body bytes are
func (r *ForwardReceiver) handleAppendObjectStream(dg *DataGroup, args []byte, body io.Reader) []byte {
	aa := raftpb.GetRootAsAppendObjectForwardArgs(args, 0)
	bucket := string(aa.Bucket())
	key := string(aa.Key())
	ctx := contextForForwardedGroup(context.Background(), dg)
	obj, err := dg.Backend().AppendObject(ctx, bucket, key, aa.ExpectedOffset(), body)
	if err != nil {
		return statusReply(mapErrorToStatus(err))
	}
	return buildObjectReply(obj, bucket)
}

func (r *ForwardReceiver) handleCompleteMultipartUpload(dg *DataGroup, args []byte) []byte {
	ctx := contextForForwardedGroup(context.Background(), dg)
	ca := raftpb.GetRootAsCompleteMultipartUploadArgs(args, 0)
	bucket := string(ca.Bucket())
	key := string(ca.Key())
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
	return buildObjectReply(obj, bucket)
}

func (r *ForwardReceiver) handleAbortMultipartUpload(dg *DataGroup, args []byte) []byte {
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
	return buildOKReply()
}

func (r *ForwardReceiver) handleListMultipartUploads(dg *DataGroup, args []byte) []byte {
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
	return buildMultipartUploadsReply(uploads)
}

func (r *ForwardReceiver) handleListParts(dg *DataGroup, args []byte) []byte {
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
	return buildPartsReply(parts)
}

func errReply(status raftpb.ForwardStatus, hint string) []byte {
	return buildSimpleReply(status, hint)
}

func statusReply(status raftpb.ForwardStatus) []byte {
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
	if errors.Is(err, storage.ErrContentMD5Mismatch) {
		return raftpb.ForwardStatusBadDigest
	}
	log.Warn().Err(err).Msg("forward receiver mapped backend error to internal status")
	return raftpb.ForwardStatusInternal
}

func (r *ForwardReceiver) handleScrubSessionStat(fbsArgs []byte) []byte {
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

func buildScrubSessionStatReply(found bool, sess scrubber.Session) []byte {
	b := flatbuffers.NewBuilder(256)
	bktOff := b.CreateString(sess.Bucket)
	pfxOff := b.CreateString(sess.KeyPrefix)
	statusOff := b.CreateString(sess.Status)

	raftpb.ScrubSessionStatReplyStart(b)
	raftpb.ScrubSessionStatReplyAddFound(b, found)
	raftpb.ScrubSessionStatReplyAddBucket(b, bktOff)
	raftpb.ScrubSessionStatReplyAddKeyPrefix(b, pfxOff)
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
	return b.FinishedBytes()
}
