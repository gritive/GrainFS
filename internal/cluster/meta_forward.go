package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

var (
	metaForwardRequestMagic  = []byte("GFSMFWD2")
	metaForwardLegacyV1Magic = []byte("GFSMFWD1")
)

type MetaForwardDialer func(peer string, payload []byte) ([]byte, error)

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
		return 0, icebergcatalog.ErrServiceUnavailable
	}
	var lastErr error
	for _, peer := range peers {
		reply, err := s.dialer(peer, payload)
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
		return 0, fmt.Errorf("%w: %v", icebergcatalog.ErrServiceUnavailable, lastErr)
	}
	return 0, icebergcatalog.ErrServiceUnavailable
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

// metaCatalogReadBuilderPool reuses FlatBuffers builders across MetaCatalogRead
// encode calls. 4096-byte initial size: LoadTable reply can carry up to ~64KB
// of Iceberg metadata. Starting at 256 forces ~9 doublings; 4KB cuts that to
// ~5 while still amortizing across small LoadNamespace replies.
var metaCatalogReadBuilderPool = pool.New(func() *flatbuffers.Builder {
	return flatbuffers.NewBuilder(4096)
})

func newMetaCatalogReadBuilder() *flatbuffers.Builder {
	b := metaCatalogReadBuilderPool.Get()
	b.Reset()
	return b
}

func releaseMetaCatalogReadBuilder(b *flatbuffers.Builder) {
	metaCatalogReadBuilderPool.Put(b)
}

func catalogOpToFB(op string) clusterpb.CatalogReadOp {
	switch op {
	case "load-namespace":
		return clusterpb.CatalogReadOpLoadNamespace
	case "list-namespaces":
		return clusterpb.CatalogReadOpListNamespaces
	case "load-table":
		return clusterpb.CatalogReadOpLoadTable
	case "list-tables":
		return clusterpb.CatalogReadOpListTables
	default:
		return clusterpb.CatalogReadOpUnknown
	}
}

func catalogOpFromFB(op clusterpb.CatalogReadOp) string {
	switch op {
	case clusterpb.CatalogReadOpLoadNamespace:
		return "load-namespace"
	case clusterpb.CatalogReadOpListNamespaces:
		return "list-namespaces"
	case clusterpb.CatalogReadOpLoadTable:
		return "load-table"
	case clusterpb.CatalogReadOpListTables:
		return "list-tables"
	default:
		return ""
	}
}

var metaCatalogReadRequestMagic = []byte("GFSMCR2")

// buildStringSliceVec writes a []string vector and returns its offset (or 0
// when empty).
func buildStringSliceVec(b *flatbuffers.Builder, ss []string) flatbuffers.UOffsetT {
	if len(ss) == 0 {
		return 0
	}
	offs := make([]flatbuffers.UOffsetT, len(ss))
	for i, s := range ss {
		offs[i] = b.CreateString(s)
	}
	b.StartVector(4, len(offs), 4)
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

// buildCatalogIdentifier returns 0 (absent) when id is zero-valued (Name == ""
// AND len(Namespace) == 0). The request encoder skips this table on
// non-LoadTable ops to avoid carrying an empty CatalogIdentifier vtable.
func buildCatalogIdentifier(b *flatbuffers.Builder, id icebergcatalog.Identifier) flatbuffers.UOffsetT {
	if id.Name == "" && len(id.Namespace) == 0 {
		return 0
	}
	nsOff := buildStringSliceVec(b, id.Namespace)
	nameOff := b.CreateString(id.Name)
	clusterpb.CatalogIdentifierStart(b)
	if nsOff != 0 {
		clusterpb.CatalogIdentifierAddNamespace(b, nsOff)
	}
	clusterpb.CatalogIdentifierAddName(b, nameOff)
	return clusterpb.CatalogIdentifierEnd(b)
}

func buildCatalogKVVec(b *flatbuffers.Builder, m map[string]string) flatbuffers.UOffsetT {
	if len(m) == 0 {
		return 0
	}
	// Sort keys for deterministic encoding (easier diffs and stable tests).
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	offs := make([]flatbuffers.UOffsetT, len(keys))
	for i, k := range keys {
		kOff := b.CreateString(k)
		vOff := b.CreateString(m[k])
		clusterpb.CatalogKVStart(b)
		clusterpb.CatalogKVAddK(b, kOff)
		clusterpb.CatalogKVAddV(b, vOff)
		offs[i] = clusterpb.CatalogKVEnd(b)
	}
	b.StartVector(4, len(offs), 4)
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func buildCatalogNamespaceVec(b *flatbuffers.Builder, nss [][]string) flatbuffers.UOffsetT {
	if len(nss) == 0 {
		return 0
	}
	offs := make([]flatbuffers.UOffsetT, len(nss))
	for i, parts := range nss {
		partsOff := buildStringSliceVec(b, parts)
		clusterpb.CatalogNamespaceStart(b)
		if partsOff != 0 {
			clusterpb.CatalogNamespaceAddParts(b, partsOff)
		}
		offs[i] = clusterpb.CatalogNamespaceEnd(b)
	}
	b.StartVector(4, len(offs), 4)
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func buildCatalogIdentifierVec(b *flatbuffers.Builder, ids []icebergcatalog.Identifier) flatbuffers.UOffsetT {
	if len(ids) == 0 {
		return 0
	}
	offs := make([]flatbuffers.UOffsetT, len(ids))
	for i, id := range ids {
		offs[i] = buildCatalogIdentifier(b, id)
	}
	b.StartVector(4, len(offs), 4)
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func buildCatalogTable(b *flatbuffers.Builder, t *icebergcatalog.Table) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	idOff := buildCatalogIdentifier(b, t.Identifier)
	locOff := b.CreateString(t.MetadataLocation)
	var metaOff flatbuffers.UOffsetT
	if len(t.Metadata) > 0 {
		metaOff = b.CreateByteVector([]byte(t.Metadata))
	}
	propsOff := buildCatalogKVVec(b, t.Properties)
	clusterpb.CatalogTableStart(b)
	if idOff != 0 {
		clusterpb.CatalogTableAddIdentifier(b, idOff)
	}
	clusterpb.CatalogTableAddMetadataLocation(b, locOff)
	if metaOff != 0 {
		clusterpb.CatalogTableAddMetadata(b, metaOff)
	}
	if propsOff != 0 {
		clusterpb.CatalogTableAddProperties(b, propsOff)
	}
	return clusterpb.CatalogTableEnd(b)
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
	case compat.OperationNfsExportCreate:
		return clusterpb.CompatOperationNfsExportCreate
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
	case clusterpb.CompatOperationNfsExportCreate:
		return compat.OperationNfsExportCreate
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

func (r *MetaProposeForwardReceiver) Handle(req *transport.Message) *transport.Message {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	var idx uint64
	command, plan, framed, decodeErr := decodeMetaForwardRequest(req.Payload)
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
	} else if isIcebergMetaCommand(command) {
		err = r.meta.ProposeMetaCommand(ctx, command)
	} else {
		idx, err = r.meta.ProposeMetaCommandWithIndex(ctx, command)
	}
	return &transport.Message{Type: transport.StreamMetaProposeForward, Payload: encodeMetaForwardReplyWithIndex(idx, err)}
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
			icebergcatalog.ErrServiceUnavailable)
	}
	if !bytes.HasPrefix(payload, metaForwardRequestMagic) {
		return payload, nil, false, nil
	}
	body := payload[len(metaForwardRequestMagic):]

	// Wrap FB panics (malformed buffer) as ErrServiceUnavailable.
	var parseErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				parseErr = fmt.Errorf("%w: malformed MetaForwardRequest: %v",
					icebergcatalog.ErrServiceUnavailable, r)
			}
		}()
		req := clusterpb.GetRootAsMetaForwardRequest(body, 0)
		// Copy command bytes — do not alias the builder-pooled buffer.
		raw := req.CommandBytes()
		if len(raw) == 0 {
			parseErr = fmt.Errorf("%w: empty gated forward command", icebergcatalog.ErrServiceUnavailable)
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

//nolint:unused // referenced by cluster_config_followerforward_test.go and iceberg_catalog_test.go.
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
					icebergcatalog.ErrServiceUnavailable, r)
			}
		}()
		reply := clusterpb.GetRootAsMetaForwardReply(data, 0)
		errType := string(reply.ErrorType())
		if errType == "" {
			idx = reply.Index()
			return
		}
		parseErr = errorFromIcebergType(errType, string(reply.ErrorMessage()))
	}()
	if parseErr != nil {
		return 0, parseErr
	}
	return idx, nil
}

// MetaForwardApplyError preserves non-Iceberg FSM apply errors across the
// follower-to-leader forwarding boundary. Callers can inspect the message today;
// future typed NFS errors can add explicit wire tags beside this generic shape.
type MetaForwardApplyError struct {
	Message string
}

func (e MetaForwardApplyError) Error() string {
	if e.Message == "" {
		return "meta apply error"
	}
	return e.Message
}

func isIcebergMetaCommand(data []byte) bool {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	switch cmd.Type() {
	case MetaCmdTypeIcebergCreateNamespace,
		MetaCmdTypeIcebergDeleteNamespace,
		MetaCmdTypeIcebergCreateTable,
		MetaCmdTypeIcebergCommitTable,
		MetaCmdTypeIcebergDeleteTable:
		return true
	default:
		return false
	}
}

func metaCommandGateRequirement(data []byte) (string, compat.Operation, bool) {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	switch cmd.Type() {
	case MetaCmdTypeNfsExportCreate:
		return compat.CapabilityNfsExportCreateV1, compat.OperationNfsExportCreate, true
	default:
		return "", "", false
	}
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
	if isIcebergCatalogError(err) {
		return icebergErrorType(err)
	}
	return "meta-apply-error"
}

func isIcebergCatalogError(err error) bool {
	return errors.Is(err, icebergcatalog.ErrNamespaceNotFound) ||
		errors.Is(err, icebergcatalog.ErrNamespaceExists) ||
		errors.Is(err, icebergcatalog.ErrNamespaceNotEmpty) ||
		errors.Is(err, icebergcatalog.ErrTableNotFound) ||
		errors.Is(err, icebergcatalog.ErrTableExists) ||
		errors.Is(err, icebergcatalog.ErrCommitFailed)
}

func icebergErrorType(err error) string {
	switch {
	case errors.Is(err, icebergcatalog.ErrNamespaceNotFound):
		return "namespace-not-found"
	case errors.Is(err, icebergcatalog.ErrNamespaceExists):
		return "namespace-exists"
	case errors.Is(err, icebergcatalog.ErrNamespaceNotEmpty):
		return "namespace-not-empty"
	case errors.Is(err, icebergcatalog.ErrTableNotFound):
		return "table-not-found"
	case errors.Is(err, icebergcatalog.ErrTableExists):
		return "table-exists"
	case errors.Is(err, icebergcatalog.ErrCommitFailed):
		return "commit-failed"
	case errors.Is(err, raft.ErrNotLeader):
		return "not-leader"
	default:
		return "service-unavailable"
	}
}

func errorFromIcebergType(errorType, message string) error {
	switch errorType {
	case "namespace-not-found":
		return icebergcatalog.ErrNamespaceNotFound
	case "namespace-exists":
		return icebergcatalog.ErrNamespaceExists
	case "namespace-not-empty":
		return icebergcatalog.ErrNamespaceNotEmpty
	case "table-not-found":
		return icebergcatalog.ErrTableNotFound
	case "table-exists":
		return icebergcatalog.ErrTableExists
	case "commit-failed":
		return icebergcatalog.ErrCommitFailed
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
			return icebergcatalog.ErrServiceUnavailable
		}
		return fmt.Errorf("%w: %s", icebergcatalog.ErrServiceUnavailable, message)
	}
}

type MetaCatalogReadSender struct {
	dialer MetaForwardDialer
}

func NewMetaCatalogReadSender(d MetaForwardDialer) *MetaCatalogReadSender {
	return &MetaCatalogReadSender{dialer: d}
}

func (s *MetaCatalogReadSender) LoadNamespace(ctx context.Context, peers []string, namespace []string) (map[string]string, error) {
	reply, err := s.send(ctx, peers, metaCatalogReadRequest{Op: "load-namespace", Namespace: namespace})
	if err != nil {
		return nil, err
	}
	return cloneStringMap(reply.Properties), nil
}

func (s *MetaCatalogReadSender) ListNamespaces(ctx context.Context, peers []string) ([][]string, error) {
	reply, err := s.send(ctx, peers, metaCatalogReadRequest{Op: "list-namespaces"})
	if err != nil {
		return nil, err
	}
	return reply.Namespaces, nil
}

func (s *MetaCatalogReadSender) LoadTable(ctx context.Context, peers []string, ident icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	reply, err := s.send(ctx, peers, metaCatalogReadRequest{Op: "load-table", Identifier: ident})
	if err != nil {
		return nil, err
	}
	if reply.Table == nil {
		return nil, icebergcatalog.ErrServiceUnavailable
	}
	return reply.Table, nil
}

func (s *MetaCatalogReadSender) ListTables(ctx context.Context, peers []string, namespace []string) ([]icebergcatalog.Identifier, error) {
	reply, err := s.send(ctx, peers, metaCatalogReadRequest{Op: "list-tables", Namespace: namespace})
	if err != nil {
		return nil, err
	}
	return reply.Tables, nil
}

func (s *MetaCatalogReadSender) send(ctx context.Context, peers []string, request metaCatalogReadRequest) (*metaLoadTableReply, error) {
	req, err := encodeMetaCatalogReadRequest(request)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, icebergcatalog.ErrServiceUnavailable
	}
	var lastErr error
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reply, err := s.dialer(peer, req)
		if err != nil {
			lastErr = err
			continue
		}
		return decodeMetaLoadTableReply(reply)
	}
	if lastErr != nil {
		return nil, fmt.Errorf("%w: %v", icebergcatalog.ErrServiceUnavailable, lastErr)
	}
	return nil, icebergcatalog.ErrServiceUnavailable
}

type MetaCatalogReadReceiver struct {
	catalog *MetaCatalog
}

func NewMetaCatalogReadReceiver(catalog *MetaCatalog) *MetaCatalogReadReceiver {
	return &MetaCatalogReadReceiver{catalog: catalog}
}

func (r *MetaCatalogReadReceiver) Handle(req *transport.Message) *transport.Message {
	reply := &metaLoadTableReply{}
	request, decodeErr := decodeMetaCatalogReadRequest(req.Payload)
	if decodeErr != nil {
		return &transport.Message{
			Type:    transport.StreamMetaCatalogRead,
			Payload: encodeMetaLoadTableReply(reply, decodeErr),
		}
	}
	var err error
	if !r.catalog.meta.IsLeader() {
		err = raft.ErrNotLeader
	} else {
		switch request.Op {
		case "load-namespace":
			reply.Properties, err = r.catalog.loadNamespaceLocal(request.Namespace)
		case "list-namespaces":
			reply.Namespaces = r.catalog.listNamespacesLocal()
		case "load-table":
			reply.Table, err = r.catalog.loadTableLocal(request.Identifier)
		case "list-tables":
			reply.Tables, err = r.catalog.listTablesLocal(request.Namespace)
		default:
			err = icebergcatalog.ErrServiceUnavailable
		}
	}
	return &transport.Message{Type: transport.StreamMetaCatalogRead, Payload: encodeMetaLoadTableReply(reply, err)}
}

type metaCatalogReadRequest struct {
	Op         string                    `json:"op"`
	Namespace  []string                  `json:"namespace,omitempty"`
	Identifier icebergcatalog.Identifier `json:"identifier,omitempty"`
}

type metaLoadTableReply struct {
	ErrorType    string                      `json:"error_type,omitempty"`
	ErrorMessage string                      `json:"error_message,omitempty"`
	Properties   map[string]string           `json:"properties,omitempty"`
	Namespaces   [][]string                  `json:"namespaces,omitempty"`
	Table        *icebergcatalog.Table       `json:"table,omitempty"`
	Tables       []icebergcatalog.Identifier `json:"tables,omitempty"`
}

func encodeMetaCatalogReadRequest(req metaCatalogReadRequest) ([]byte, error) {
	b := newMetaCatalogReadBuilder()
	defer releaseMetaCatalogReadBuilder(b)

	nsOff := buildStringSliceVec(b, req.Namespace)
	// Only build identifier when the request actually uses it (LoadTable).
	// For other ops the zero-value identifier returns 0 offset (absent table),
	// keeping per-call payload size lean.
	var idOff flatbuffers.UOffsetT
	if req.Op == "load-table" {
		idOff = buildCatalogIdentifier(b, req.Identifier)
	}

	clusterpb.MetaCatalogReadRequestStart(b)
	clusterpb.MetaCatalogReadRequestAddOp(b, catalogOpToFB(req.Op))
	if nsOff != 0 {
		clusterpb.MetaCatalogReadRequestAddNamespace(b, nsOff)
	}
	if idOff != 0 {
		clusterpb.MetaCatalogReadRequestAddIdentifier(b, idOff)
	}
	b.Finish(clusterpb.MetaCatalogReadRequestEnd(b))
	fb := b.FinishedBytes()

	out := make([]byte, 0, len(metaCatalogReadRequestMagic)+len(fb))
	out = append(out, metaCatalogReadRequestMagic...)
	out = append(out, fb...)
	return out, nil
}

func decodeMetaCatalogReadRequest(payload []byte) (req metaCatalogReadRequest, err error) {
	// Mixed-version diagnostic: an old sender encodes raw JSON; surface that
	// as a typed error rather than a confused "malformed FB" symptom.
	if len(payload) > 0 && payload[0] == '{' {
		return metaCatalogReadRequest{}, fmt.Errorf("%w: peer sent legacy JSON request (mixed-version)", icebergcatalog.ErrServiceUnavailable)
	}
	if !bytes.HasPrefix(payload, metaCatalogReadRequestMagic) {
		return metaCatalogReadRequest{}, fmt.Errorf("%w: bad meta_catalog_read magic", icebergcatalog.ErrServiceUnavailable)
	}
	// Per-call defer recover, mirrors PR #413 decodeMetaForwardRequest.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: malformed MetaCatalogReadRequest: %v", icebergcatalog.ErrServiceUnavailable, r)
		}
	}()

	fb := clusterpb.GetRootAsMetaCatalogReadRequest(payload[len(metaCatalogReadRequestMagic):], 0)
	req.Op = catalogOpFromFB(fb.Op())
	nsLen := fb.NamespaceLength()
	if nsLen > 0 {
		req.Namespace = make([]string, nsLen)
		for i := 0; i < nsLen; i++ {
			req.Namespace[i] = string(fb.Namespace(i))
		}
	}
	if id := fb.Identifier(nil); id != nil {
		req.Identifier = icebergcatalog.Identifier{Name: string(id.Name())}
		idNsLen := id.NamespaceLength()
		if idNsLen > 0 {
			req.Identifier.Namespace = make([]string, idNsLen)
			for i := 0; i < idNsLen; i++ {
				req.Identifier.Namespace[i] = string(id.Namespace(i))
			}
		}
	}
	return req, nil
}

func encodeMetaLoadTableReply(reply *metaLoadTableReply, err error) []byte {
	if reply == nil {
		reply = &metaLoadTableReply{}
	}
	if err != nil {
		reply.ErrorType = icebergErrorType(err)
		reply.ErrorMessage = err.Error()
		reply.Table = nil
	}

	b := newMetaCatalogReadBuilder()
	defer releaseMetaCatalogReadBuilder(b)

	var (
		errTypeOff flatbuffers.UOffsetT
		errMsgOff  flatbuffers.UOffsetT
		propsOff   flatbuffers.UOffsetT
		nssOff     flatbuffers.UOffsetT
		tableOff   flatbuffers.UOffsetT
		tablesOff  flatbuffers.UOffsetT
	)
	if reply.ErrorType != "" {
		errTypeOff = b.CreateString(reply.ErrorType)
	}
	if reply.ErrorMessage != "" {
		errMsgOff = b.CreateString(reply.ErrorMessage)
	}
	propsOff = buildCatalogKVVec(b, reply.Properties)
	nssOff = buildCatalogNamespaceVec(b, reply.Namespaces)
	tableOff = buildCatalogTable(b, reply.Table)
	tablesOff = buildCatalogIdentifierVec(b, reply.Tables)

	clusterpb.MetaCatalogReadReplyStart(b)
	if errTypeOff != 0 {
		clusterpb.MetaCatalogReadReplyAddErrorType(b, errTypeOff)
	}
	if errMsgOff != 0 {
		clusterpb.MetaCatalogReadReplyAddErrorMessage(b, errMsgOff)
	}
	if propsOff != 0 {
		clusterpb.MetaCatalogReadReplyAddProperties(b, propsOff)
	}
	if nssOff != 0 {
		clusterpb.MetaCatalogReadReplyAddNamespaces(b, nssOff)
	}
	if tableOff != 0 {
		// Field is loaded_table on the wire (avoids collision with the FB
		// built-in Table() accessor); generator emits AddLoadedTable.
		clusterpb.MetaCatalogReadReplyAddLoadedTable(b, tableOff)
	}
	if tablesOff != 0 {
		clusterpb.MetaCatalogReadReplyAddTables(b, tablesOff)
	}
	b.Finish(clusterpb.MetaCatalogReadReplyEnd(b))
	return append([]byte(nil), b.FinishedBytes()...)
}

func decodeMetaLoadTableReply(data []byte) (reply *metaLoadTableReply, err error) {
	// Mixed-version diagnostic: an old receiver encodes the JSON shape.
	if len(data) > 0 && data[0] == '{' {
		return nil, fmt.Errorf("%w: peer returned legacy JSON reply (mixed-version)", icebergcatalog.ErrServiceUnavailable)
	}
	// Per-call defer recover, mirrors PR #413 decodeMetaForwardReplyWithIndex.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: malformed MetaCatalogReadReply: %v", icebergcatalog.ErrServiceUnavailable, r)
		}
	}()
	fb := clusterpb.GetRootAsMetaCatalogReadReply(data, 0)
	errType := string(fb.ErrorType())
	if errType != "" {
		return nil, errorFromIcebergType(errType, string(fb.ErrorMessage()))
	}

	r := &metaLoadTableReply{}
	if propsLen := fb.PropertiesLength(); propsLen > 0 {
		r.Properties = make(map[string]string, propsLen)
		var kv clusterpb.CatalogKV
		for i := 0; i < propsLen; i++ {
			if fb.Properties(&kv, i) {
				r.Properties[string(kv.K())] = string(kv.V())
			}
		}
	}
	if nssLen := fb.NamespacesLength(); nssLen > 0 {
		r.Namespaces = make([][]string, nssLen)
		var ns clusterpb.CatalogNamespace
		for i := 0; i < nssLen; i++ {
			if fb.Namespaces(&ns, i) {
				partsLen := ns.PartsLength()
				parts := make([]string, partsLen)
				for j := 0; j < partsLen; j++ {
					parts[j] = string(ns.Parts(j))
				}
				r.Namespaces[i] = parts
			}
		}
	}
	if tbl := fb.LoadedTable(nil); tbl != nil {
		t := &icebergcatalog.Table{
			MetadataLocation: string(tbl.MetadataLocation()),
		}
		if id := tbl.Identifier(nil); id != nil {
			t.Identifier = icebergcatalog.Identifier{Name: string(id.Name())}
			idNsLen := id.NamespaceLength()
			if idNsLen > 0 {
				t.Identifier.Namespace = make([]string, idNsLen)
				for i := 0; i < idNsLen; i++ {
					t.Identifier.Namespace[i] = string(id.Namespace(i))
				}
			}
		}
		// MetadataBytes returns the underlying FB-owned slice; copy so the
		// caller can hold onto Metadata after the FB buffer is released.
		if mb := tbl.MetadataBytes(); len(mb) > 0 {
			t.Metadata = append([]byte(nil), mb...)
		}
		if propsLen := tbl.PropertiesLength(); propsLen > 0 {
			t.Properties = make(map[string]string, propsLen)
			var kv clusterpb.CatalogKV
			for i := 0; i < propsLen; i++ {
				if tbl.Properties(&kv, i) {
					t.Properties[string(kv.K())] = string(kv.V())
				}
			}
		}
		r.Table = t
	}
	if tablesLen := fb.TablesLength(); tablesLen > 0 {
		r.Tables = make([]icebergcatalog.Identifier, tablesLen)
		var id clusterpb.CatalogIdentifier
		for i := 0; i < tablesLen; i++ {
			if fb.Tables(&id, i) {
				ident := icebergcatalog.Identifier{Name: string(id.Name())}
				idNsLen := id.NamespaceLength()
				if idNsLen > 0 {
					ident.Namespace = make([]string, idNsLen)
					for j := 0; j < idNsLen; j++ {
						ident.Namespace[j] = string(id.Namespace(j))
					}
				}
				r.Tables[i] = ident
			}
		}
	}
	return r, nil
}
