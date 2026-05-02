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
	dialer  MetaForwardDialer
	timeout time.Duration
}

func NewMetaProposeForwardSender(d MetaForwardDialer) *MetaProposeForwardSender {
	return &MetaProposeForwardSender{dialer: d, timeout: 10 * time.Second}
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
