package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

func performMetaJoin(ctx context.Context, quicTransport *transport.QUICTransport, peers []string, nodeID, raftAddr string) error {
	joinCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	sender := cluster.NewMetaJoinSender(func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoin, Payload: payload}
		reply, err := quicTransport.Call(joinCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})
	reply, err := sender.SendJoin(joinCtx, peers, cluster.JoinRequest{NodeID: nodeID, Address: raftAddr})
	if err != nil {
		return fmt.Errorf("meta join: %w", err)
	}
	if !reply.Accepted {
		if reply.Message != "" {
			return fmt.Errorf("meta join rejected: %s: %s", reply.Status, reply.Message)
		}
		return fmt.Errorf("meta join rejected: %s", reply.Status)
	}
	return nil
}
