package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/rs/zerolog/log"
)

// errMetaForwardUnavailable is returned when the meta propose forward path has
// no available peers or encounters a transient service error.
var errMetaForwardUnavailable = errors.New("meta forward: service unavailable")

var (
	metaForwardRequestMagic  = []byte("GFSMFWD2")
	metaForwardLegacyV1Magic = []byte("GFSMFWD1")
)

type MetaForwardDialer func(ctx context.Context, peer string, payload []byte) ([]byte, error)

type MetaProposeForwardSender struct {
	dialer MetaForwardDialer
}

func NewMetaProposeForwardSender(d MetaForwardDialer) *MetaProposeForwardSender {
	return &MetaProposeForwardSender{dialer: d}
}

func (s *MetaProposeForwardSender) Send(ctx context.Context, peers []string, command []byte) error {
	_, err := s.SendWithIndex(ctx, peers, command)
	return err
}

func (s *MetaProposeForwardSender) SendWithIndex(ctx context.Context, peers []string, command []byte) (uint64, error) {
	return s.sendPayloadWithIndex(ctx, peers, command)
}

func (s *MetaProposeForwardSender) SendWithGate(ctx context.Context, peers []string, command []byte, plan compat.GatePlan) (uint64, error) {
	return s.sendPayloadWithIndex(ctx, peers, encodeMetaForwardRequest(command, &plan))
}

func (s *MetaProposeForwardSender) sendPayloadWithIndex(ctx context.Context, peers []string, payload []byte) (uint64, error) {
	if len(peers) == 0 {
		log.Warn().
			Str("component", "meta-forward").
			Str("path", "empty-peers").
			Msg("meta-forward: no peers to forward to — likely no known leader; returning 503")
		return 0, errMetaForwardUnavailable
	}
	var lastErr error
	for _, peer := range peers {
		reply, err := s.dialer(ctx, peer, payload)
		if err != nil {
			lastErr = err
			continue
		}
		idx, err := decodeMetaForwardReplyWithIndex(reply)
		if errors.Is(err, raft.ErrNotLeader) {
			lastErr = err
			continue
		}
		return idx, err
	}
	if lastErr != nil {
		log.Warn().
			Str("component", "meta-forward").
			Str("path", "all-peers-failed").
			Int("peers_tried", len(peers)).
			Err(lastErr).
			Msg("meta-forward: every peer rejected the proposal; returning 503")
		return 0, fmt.Errorf("%w: %v", errMetaForwardUnavailable, lastErr)
	}
	log.Warn().
		Str("component", "meta-forward").
		Str("path", "no-error-no-success").
		Int("peers_tried", len(peers)).
		Msg("meta-forward: loop exited without success or recorded error (unreachable); returning 503")
	return 0, errMetaForwardUnavailable
}

// metaForwardBuilderPool reuses FlatBuffers builders across calls to avoid
// per-call allocation churn on the hot forward path.
var metaForwardBuilderPool = pool.New(func() *flatbuffers.Builder {
	return flatbuffers.NewBuilder(256)
})

func newMetaForwardBuilder() *flatbuffers.Builder {
	b := metaForwardBuilderPool.Get()
	b.Reset()
	return b
}

func releaseMetaForwardBuilder(b *flatbuffers.Builder) {
	metaForwardBuilderPool.Put(b)
}

// scopeToFB converts a compat.Scope to its FlatBuffers enum value.
func scopeToFB(s compat.Scope) clusterpb.CompatScope {
	switch s {
	case compat.ScopeMetaRaft:
		return clusterpb.CompatScopeMetaRaft
	case compat.ScopeDataGroup:
		return clusterpb.CompatScopeDataGroup
	case compat.ScopePeerTransport:
		return clusterpb.CompatScopePeerTransport
	case compat.ScopeLocal:
		return clusterpb.CompatScopeLocal
	default:
		return clusterpb.CompatScopeUnknown
	}
}

// scopeFromFB converts a FlatBuffers CompatScope to compat.Scope.
func scopeFromFB(s clusterpb.CompatScope) compat.Scope {
	switch s {
	case clusterpb.CompatScopeMetaRaft:
		return compat.ScopeMetaRaft
	case clusterpb.CompatScopeDataGroup:
		return compat.ScopeDataGroup
	case clusterpb.CompatScopePeerTransport:
		return compat.ScopePeerTransport
	case clusterpb.CompatScopeLocal:
		return compat.ScopeLocal
	default:
		return ""
	}
}

// severityToFB converts a compat.Severity to its FlatBuffers enum value.
func severityToFB(s compat.Severity) clusterpb.CompatSeverity {
	switch s {
	case compat.SeverityHard:
		return clusterpb.CompatSeverityHard
	case compat.SeveritySoft:
		return clusterpb.CompatSeveritySoft
	default:
		return clusterpb.CompatSeverityUnknown
	}
}

// severityFromFB converts a FlatBuffers CompatSeverity to compat.Severity.
func severityFromFB(s clusterpb.CompatSeverity) compat.Severity {
	switch s {
	case clusterpb.CompatSeverityHard:
		return compat.SeverityHard
	case clusterpb.CompatSeveritySoft:
		return compat.SeveritySoft
	default:
		return ""
	}
}

// operationToFB converts a compat.Operation to its FlatBuffers enum value.
func operationToFB(o compat.Operation) clusterpb.CompatOperation {
	switch o {
	case compat.OperationMigrationCutover:
		return clusterpb.CompatOperationMigrationCutover
	case compat.OperationCreateMultipartUpload:
		return clusterpb.CompatOperationCreateMultipartUpload
	case compat.OperationListMultipartUploads:
		return clusterpb.CompatOperationListMultipartUploads
	case compat.OperationListParts:
		return clusterpb.CompatOperationListParts
	default:
		return clusterpb.CompatOperationUnknown
	}
}

// operationFromFB converts a FlatBuffers CompatOperation to compat.Operation.
func operationFromFB(o clusterpb.CompatOperation) compat.Operation {
	switch o {
	case clusterpb.CompatOperationMigrationCutover:
		return compat.OperationMigrationCutover
	case clusterpb.CompatOperationCreateMultipartUpload:
		return compat.OperationCreateMultipartUpload
	case clusterpb.CompatOperationListMultipartUploads:
		return compat.OperationListMultipartUploads
	case clusterpb.CompatOperationListParts:
		return compat.OperationListParts
	default:
		return ""
	}
}

// buildStringVec serializes a []NodeID as a FlatBuffers [string] vector.
func buildStringVec(b *flatbuffers.Builder, nodes []compat.NodeID) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(nodes))
	for i, n := range nodes {
		offsets[i] = b.CreateString(string(n))
	}
	// All CompatGatePlanStart*Vector calls use (4, n, 4) — safe to use any of them.
	clusterpb.CompatGatePlanStartRequiredVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

// buildCompatGatePlan serializes a *compat.GatePlan into the builder and
// returns the resulting table offset. Returns 0 if plan is nil.
func buildCompatGatePlan(b *flatbuffers.Builder, plan *compat.GatePlan) flatbuffers.UOffsetT {
	if plan == nil {
		return 0
	}

	// Build all strings and vectors before starting the table object.
	capOff := b.CreateString(plan.Capability)

	var reqOff, misOff, unkOff, staleOff flatbuffers.UOffsetT
	if len(plan.Required) > 0 {
		reqOff = buildStringVec(b, plan.Required)
	}
	if len(plan.Missing) > 0 {
		misOff = buildStringVec(b, plan.Missing)
	}
	if len(plan.Unknown) > 0 {
		unkOff = buildStringVec(b, plan.Unknown)
	}
	if len(plan.Stale) > 0 {
		// Build StaleNode tables first (bottom-up).
		staleOffsets := make([]flatbuffers.UOffsetT, len(plan.Stale))
		for i, sn := range plan.Stale {
			nodeID := b.CreateString(string(sn.NodeID))
			clusterpb.StaleNodeStart(b)
			clusterpb.StaleNodeAddNodeId(b, nodeID)
			clusterpb.StaleNodeAddLastSeenUnixMs(b, sn.LastSeen.UnixMilli())
			staleOffsets[i] = clusterpb.StaleNodeEnd(b)
		}
		clusterpb.CompatGatePlanStartStaleVector(b, len(staleOffsets))
		for i := len(staleOffsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(staleOffsets[i])
		}
		staleOff = b.EndVector(len(staleOffsets))
	}

	clusterpb.CompatGatePlanStart(b)
	clusterpb.CompatGatePlanAddCapability(b, capOff)
	clusterpb.CompatGatePlanAddScope(b, scopeToFB(plan.Scope))
	clusterpb.CompatGatePlanAddSeverity(b, severityToFB(plan.Severity))
	clusterpb.CompatGatePlanAddOperation(b, operationToFB(plan.Operation))
	clusterpb.CompatGatePlanAddConfigId(b, plan.ConfigID)
	if reqOff != 0 {
		clusterpb.CompatGatePlanAddRequired(b, reqOff)
	}
	if misOff != 0 {
		clusterpb.CompatGatePlanAddMissing(b, misOff)
	}
	if unkOff != 0 {
		clusterpb.CompatGatePlanAddUnknown(b, unkOff)
	}
	if staleOff != 0 {
		clusterpb.CompatGatePlanAddStale(b, staleOff)
	}
	return clusterpb.CompatGatePlanEnd(b)
}

// decodeCompatGatePlan converts a FlatBuffers CompatGatePlan to a *compat.GatePlan.
func decodeCompatGatePlan(fb *clusterpb.CompatGatePlan) *compat.GatePlan {
	if fb == nil {
		return nil
	}
	plan := &compat.GatePlan{
		Capability: string(fb.Capability()),
		Scope:      scopeFromFB(fb.Scope()),
		Severity:   severityFromFB(fb.Severity()),
		Operation:  operationFromFB(fb.Operation()),
		ConfigID:   fb.ConfigId(),
	}
	if n := fb.RequiredLength(); n > 0 {
		plan.Required = make([]compat.NodeID, n)
		for i := range n {
			plan.Required[i] = compat.NodeID(fb.Required(i))
		}
	}
	if n := fb.MissingLength(); n > 0 {
		plan.Missing = make([]compat.NodeID, n)
		for i := range n {
			plan.Missing[i] = compat.NodeID(fb.Missing(i))
		}
	}
	if n := fb.UnknownLength(); n > 0 {
		plan.Unknown = make([]compat.NodeID, n)
		for i := range n {
			plan.Unknown[i] = compat.NodeID(fb.Unknown(i))
		}
	}
	if n := fb.StaleLength(); n > 0 {
		plan.Stale = make([]compat.StaleNode, n)
		for i := range n {
			var sn clusterpb.StaleNode
			if fb.Stale(&sn, i) {
				plan.Stale[i] = compat.StaleNode{
					NodeID:   compat.NodeID(sn.NodeId()),
					LastSeen: time.UnixMilli(sn.LastSeenUnixMs()).UTC(),
				}
			}
		}
	}
	return plan
}

type MetaProposeForwardReceiver struct {
	meta        *MetaRaft
	gateRefresh func()
}

func NewMetaProposeForwardReceiver(meta *MetaRaft) *MetaProposeForwardReceiver {
	return &MetaProposeForwardReceiver{meta: meta}
}

func (r *MetaProposeForwardReceiver) WithGateRefresh(fn func()) *MetaProposeForwardReceiver {
	r.gateRefresh = fn
	return r
}

// Handle serves one buffered /raft/meta/propose request. The propose outcome
// (index + error) is in-band via encodeMetaForwardReplyWithIndex; the returned
// error is always nil.
func (r *MetaProposeForwardReceiver) Handle(payload []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	var idx uint64
	command, plan, framed, decodeErr := decodeMetaForwardRequest(payload)
	if decodeErr != nil {
		err = decodeErr
	} else if framed && plan == nil {
		err = fmt.Errorf("meta forward: framed request missing gate plan")
	} else if !r.meta.IsLeader() {
		err = raft.ErrNotLeader
	} else if plan != nil {
		if r.gateRefresh != nil {
			r.gateRefresh()
		}
		idx, err = r.meta.ProposeMetaCommandWithGate(ctx, *plan, command)
	} else if capability, operation, ok := metaCommandGateRequirement(command); ok {
		err = compat.Reject(compat.GatePlan{
			Capability: capability,
			Scope:      compat.ScopeMetaRaft,
			Severity:   compat.SeverityHard,
			Operation:  operation,
			Unknown:    []compat.NodeID{"gate_plan"},
		})
	} else {
		idx, err = r.meta.ProposeMetaCommandWithIndex(ctx, command)
	}
	return encodeMetaForwardReplyWithIndex(idx, err), nil
}

func encodeMetaForwardRequest(command []byte, plan *compat.GatePlan) []byte {
	b := newMetaForwardBuilder()
	defer releaseMetaForwardBuilder(b)

	planOff := buildCompatGatePlan(b, plan)
	cmdOff := b.CreateByteVector(command)

	clusterpb.MetaForwardRequestStart(b)
	clusterpb.MetaForwardRequestAddCommand(b, cmdOff)
	if planOff != 0 {
		clusterpb.MetaForwardRequestAddGatePlan(b, planOff)
	}
	b.Finish(clusterpb.MetaForwardRequestEnd(b))
	body := b.FinishedBytes()

	out := make([]byte, 0, len(metaForwardRequestMagic)+len(body))
	out = append(out, metaForwardRequestMagic...)
	out = append(out, body...)
	return out
}

func decodeMetaForwardRequest(payload []byte) (command []byte, plan *compat.GatePlan, framed bool, err error) {
	if bytes.HasPrefix(payload, metaForwardLegacyV1Magic) {
		return nil, nil, true, fmt.Errorf("%w: legacy GFSMFWD1 wire format no longer supported; upgrade all nodes",
			errMetaForwardUnavailable)
	}
	if !bytes.HasPrefix(payload, metaForwardRequestMagic) {
		return payload, nil, false, nil
	}
	body := payload[len(metaForwardRequestMagic):]

	// Wrap FB panics (malformed buffer) as errMetaForwardUnavailable.
	var parseErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				parseErr = fmt.Errorf("%w: malformed MetaForwardRequest: %v",
					errMetaForwardUnavailable, r)
			}
		}()
		req := clusterpb.GetRootAsMetaForwardRequest(body, 0)
		// Copy command bytes — do not alias the builder-pooled buffer.
		raw := req.CommandBytes()
		if len(raw) == 0 {
			parseErr = fmt.Errorf("%w: empty gated forward command", errMetaForwardUnavailable)
			return
		}
		command = append([]byte(nil), raw...)
		plan = decodeCompatGatePlan(req.GatePlan(nil))
	}()
	if parseErr != nil {
		return nil, nil, true, parseErr
	}
	return command, plan, true, nil
}

//nolint:unused // referenced by cluster_config_followerforward_test.go.
func encodeMetaForwardReply(err error) []byte {
	return encodeMetaForwardReplyWithIndex(0, err)
}

func encodeMetaForwardReplyWithIndex(index uint64, err error) []byte {
	b := newMetaForwardBuilder()
	defer releaseMetaForwardBuilder(b)

	var errTypeOff, errMsgOff flatbuffers.UOffsetT
	if err != nil {
		errTypeOff = b.CreateString(metaForwardErrorType(err))
		errMsgOff = b.CreateString(err.Error())
	}

	clusterpb.MetaForwardReplyStart(b)
	clusterpb.MetaForwardReplyAddIndex(b, index)
	if err != nil {
		clusterpb.MetaForwardReplyAddErrorType(b, errTypeOff)
		clusterpb.MetaForwardReplyAddErrorMessage(b, errMsgOff)
	}
	b.Finish(clusterpb.MetaForwardReplyEnd(b))
	return append([]byte(nil), b.FinishedBytes()...)
}

func decodeMetaForwardReplyWithIndex(data []byte) (uint64, error) {
	var (
		idx      uint64
		parseErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				parseErr = fmt.Errorf("%w: malformed MetaForwardReply: %v",
					errMetaForwardUnavailable, r)
			}
		}()
		reply := clusterpb.GetRootAsMetaForwardReply(data, 0)
		errType := string(reply.ErrorType())
		if errType == "" {
			idx = reply.Index()
			return
		}
		parseErr = errorFromForwardType(errType, string(reply.ErrorMessage()))
	}()
	if parseErr != nil {
		return 0, parseErr
	}
	return idx, nil
}

// MetaForwardApplyError preserves FSM apply errors across the
// follower-to-leader forwarding boundary. Callers can inspect the message today;
// future typed errors can add explicit wire tags beside this generic shape.
type MetaForwardApplyError struct {
	Message string
}

func (e MetaForwardApplyError) Error() string {
	if e.Message == "" {
		return "meta apply error"
	}
	return e.Message
}

func metaCommandGateRequirement(_ []byte) (string, compat.Operation, bool) {
	return "", "", false
}

func metaForwardErrorType(err error) string {
	if errors.Is(err, raft.ErrNotLeader) {
		return "not-leader"
	}
	if errors.Is(err, compat.ErrCapabilityRejected) {
		return "capability-rejected"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "meta-apply-error"
}

func errorFromForwardType(errorType, message string) error {
	switch errorType {
	case "not-leader":
		return raft.ErrNotLeader
	case "capability-rejected":
		return fmt.Errorf("%w: %s", compat.ErrCapabilityRejected, message)
	case "timeout":
		return context.DeadlineExceeded
	case "canceled":
		return context.Canceled
	case "meta-apply-error":
		return MetaForwardApplyError{Message: message}
	default:
		if message == "" {
			return errMetaForwardUnavailable
		}
		return fmt.Errorf("%w: %s", errMetaForwardUnavailable, message)
	}
}

// ── Meta read-index forward path ─────────────────────────────────────────────
//
// Wire format mirrors the data-group ReadIndex forward (DistributedBackend):
//   request:  empty (nil payload)
//   response: [8B commitIndex big-endian][4B errLen big-endian][errBytes...]
//
// The handler (MetaReadIndexForwardReceiver.Handle) is registered on
// RouteForwardMetaReadIndex ("/raft/meta/read-index").
// The sender (MetaReadIndexForwardSender) dials that route on the leader.

// MetaReadIndexDialer dials the meta leader's read-index route.
type MetaReadIndexDialer func(ctx context.Context, leaderAddr string) ([]byte, error)

// MetaReadIndexForwardSender is the follower-side client for the meta
// read-index forward route. Wire via MetaRaft.SetReadIndexForwarder.
type MetaReadIndexForwardSender struct {
	dialer MetaReadIndexDialer
}

// NewMetaReadIndexForwardSender creates a sender backed by dialer.
func NewMetaReadIndexForwardSender(d MetaReadIndexDialer) *MetaReadIndexForwardSender {
	return &MetaReadIndexForwardSender{dialer: d}
}

// Send dials the meta leader's RouteForwardMetaReadIndex and returns the
// committed index. Returns raft.ErrNotLeader if the remote is not the leader.
func (s *MetaReadIndexForwardSender) Send(ctx context.Context, leaderAddr string) (uint64, error) {
	reply, err := s.dialer(ctx, leaderAddr)
	if err != nil {
		return 0, fmt.Errorf("meta read-index forward: %w", err)
	}
	return decodeMetaReadIndexReply(reply)
}

// MetaReadIndexForwardReceiver is the leader-side handler for
// RouteForwardMetaReadIndex. Register Handle via transport.RegisterBufferedRoute.
type MetaReadIndexForwardReceiver struct {
	meta *MetaRaft
}

// NewMetaReadIndexForwardReceiver creates a receiver backed by meta.
func NewMetaReadIndexForwardReceiver(meta *MetaRaft) *MetaReadIndexForwardReceiver {
	return &MetaReadIndexForwardReceiver{meta: meta}
}

// Handle serves one RouteForwardMetaReadIndex request. The outcome
// (commitIndex or error text) is in-band in the reply; the returned error is
// always nil so the HTTP layer returns 200 (application-level error in body).
func (r *MetaReadIndexForwardReceiver) Handle(_ []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	idx, err := r.meta.node.ReadIndex(ctx)
	return encodeMetaReadIndexReply(idx, err), nil
}

// encodeMetaReadIndexReply packs (idx, err) into the wire frame:
//
//	[8B idx BE][4B errLen BE][errBytes...]
func encodeMetaReadIndexReply(idx uint64, err error) []byte {
	var errBytes []byte
	if err != nil {
		errBytes = []byte(err.Error())
	}
	buf := make([]byte, 12+len(errBytes))
	binary.BigEndian.PutUint64(buf[0:8], idx)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(errBytes)))
	copy(buf[12:], errBytes)
	return buf
}

// decodeMetaReadIndexReply unpacks the wire frame produced by encodeMetaReadIndexReply.
func decodeMetaReadIndexReply(reply []byte) (uint64, error) {
	if len(reply) < 12 {
		return 0, fmt.Errorf("meta read-index forward: short response: %d bytes", len(reply))
	}
	idx := binary.BigEndian.Uint64(reply[0:8])
	errLen := binary.BigEndian.Uint32(reply[8:12])
	if errLen > 0 && len(reply) >= 12+int(errLen) {
		msg := string(reply[12 : 12+int(errLen)])
		if msg == raft.ErrNotLeader.Error() {
			return 0, raft.ErrNotLeader
		}
		return 0, fmt.Errorf("meta read-index forward: leader: %s", msg)
	}
	return idx, nil
}
