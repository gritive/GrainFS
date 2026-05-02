package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

type MetaForwardDialer func(peer string, payload []byte) ([]byte, error)

type MetaProposeForwardSender struct {
	dialer MetaForwardDialer
}

func NewMetaProposeForwardSender(d MetaForwardDialer) *MetaProposeForwardSender {
	return &MetaProposeForwardSender{dialer: d}
}

func (s *MetaProposeForwardSender) Send(ctx context.Context, peers []string, command []byte) error {
	if len(peers) == 0 {
		return icebergcatalog.ErrServiceUnavailable
	}
	var lastErr error
	for _, peer := range peers {
		reply, err := s.dialer(peer, command)
		if err != nil {
			lastErr = err
			continue
		}
		return decodeMetaForwardReply(reply)
	}
	if lastErr != nil {
		return fmt.Errorf("%w: %v", icebergcatalog.ErrServiceUnavailable, lastErr)
	}
	return icebergcatalog.ErrServiceUnavailable
}

type MetaProposeForwardReceiver struct {
	meta *MetaRaft
}

func NewMetaProposeForwardReceiver(meta *MetaRaft) *MetaProposeForwardReceiver {
	return &MetaProposeForwardReceiver{meta: meta}
}

func (r *MetaProposeForwardReceiver) Handle(req *transport.Message) *transport.Message {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if !r.meta.IsLeader() {
		err = raft.ErrNotLeader
	} else {
		err = r.meta.ProposeIcebergMetaCommand(ctx, req.Payload)
	}
	return &transport.Message{Type: transport.StreamMetaProposeForward, Payload: encodeMetaForwardReply(err)}
}

type metaForwardReply struct {
	ErrorType    string `json:"error_type,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func encodeMetaForwardReply(err error) []byte {
	reply := metaForwardReply{}
	if err != nil {
		reply.ErrorType = icebergErrorType(err)
		reply.ErrorMessage = err.Error()
	}
	data, _ := json.Marshal(reply)
	return data
}

func decodeMetaForwardReply(data []byte) error {
	var reply metaForwardReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return fmt.Errorf("%w: invalid forward reply: %v", icebergcatalog.ErrServiceUnavailable, err)
	}
	if reply.ErrorType == "" {
		return nil
	}
	return errorFromIcebergType(reply.ErrorType, reply.ErrorMessage)
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
