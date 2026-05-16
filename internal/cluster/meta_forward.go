package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

var metaForwardRequestMagic = []byte("GFSMFWD1")

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

type metaForwardRequest struct {
	Command  []byte           `json:"command"`
	GatePlan *compat.GatePlan `json:"gate_plan,omitempty"`
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
	body, _ := json.Marshal(metaForwardRequest{Command: command, GatePlan: plan})
	out := make([]byte, 0, len(metaForwardRequestMagic)+len(body))
	out = append(out, metaForwardRequestMagic...)
	out = append(out, body...)
	return out
}

func decodeMetaForwardRequest(payload []byte) (command []byte, plan *compat.GatePlan, framed bool, err error) {
	if !bytes.HasPrefix(payload, metaForwardRequestMagic) {
		return payload, nil, false, nil
	}
	var req metaForwardRequest
	if err := json.Unmarshal(payload[len(metaForwardRequestMagic):], &req); err != nil {
		return nil, nil, true, fmt.Errorf("%w: invalid gated forward request: %v", icebergcatalog.ErrServiceUnavailable, err)
	}
	if len(req.Command) == 0 {
		return nil, nil, true, fmt.Errorf("%w: empty gated forward command", icebergcatalog.ErrServiceUnavailable)
	}
	return req.Command, req.GatePlan, true, nil
}

type metaForwardReply struct {
	Index        uint64 `json:"index,omitempty"`
	ErrorType    string `json:"error_type,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

//nolint:unused // referenced by cluster_config_followerforward_test.go and iceberg_catalog_test.go.
func encodeMetaForwardReply(err error) []byte {
	return encodeMetaForwardReplyWithIndex(0, err)
}

func encodeMetaForwardReplyWithIndex(index uint64, err error) []byte {
	reply := metaForwardReply{Index: index}
	if err != nil {
		reply.ErrorType = metaForwardErrorType(err)
		reply.ErrorMessage = err.Error()
	}
	data, _ := json.Marshal(reply)
	return data
}

func decodeMetaForwardReplyWithIndex(data []byte) (uint64, error) {
	var reply metaForwardReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return 0, fmt.Errorf("%w: invalid forward reply: %v", icebergcatalog.ErrServiceUnavailable, err)
	}
	if reply.ErrorType == "" {
		return reply.Index, nil
	}
	return 0, errorFromIcebergType(reply.ErrorType, reply.ErrorMessage)
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
	req, err := json.Marshal(request)
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
	var request metaCatalogReadRequest
	reply := &metaLoadTableReply{}
	var err error
	if decodeErr := json.Unmarshal(req.Payload, &request); decodeErr != nil {
		err = decodeErr
	} else if !r.catalog.meta.IsLeader() {
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

func encodeMetaLoadTableReply(reply *metaLoadTableReply, err error) []byte {
	if reply == nil {
		reply = &metaLoadTableReply{}
	}
	if err != nil {
		reply.ErrorType = icebergErrorType(err)
		reply.ErrorMessage = err.Error()
		reply.Table = nil
	}
	data, _ := json.Marshal(reply)
	return data
}

func decodeMetaLoadTableReply(data []byte) (*metaLoadTableReply, error) {
	var reply metaLoadTableReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return nil, fmt.Errorf("%w: invalid load-table reply: %v", icebergcatalog.ErrServiceUnavailable, err)
	}
	if reply.ErrorType != "" {
		return nil, errorFromIcebergType(reply.ErrorType, reply.ErrorMessage)
	}
	return &reply, nil
}
