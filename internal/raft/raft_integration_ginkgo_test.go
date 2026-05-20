package raft

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func TestRaftIntegration(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Raft integration")
}

func startRaftIntegrationCluster(ids ...string) ([]*Node, *memNetwork, func(), error) {
	if len(ids) == 0 {
		return nil, nil, nil, errors.New("startRaftIntegrationCluster: ids must not be empty")
	}

	net := newMemNetwork()
	nodes := make([]*Node, 0, len(ids))
	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}

		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		node, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		if err != nil {
			return nil, nil, nil, err
		}
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		node.SetTransport(net.Register(node.cfg.ID, node))
	}

	var wg sync.WaitGroup
	for _, node := range nodes {
		node.Start()
		wg.Add(1)
		go func(node *Node) {
			defer wg.Done()
			for range node.ApplyCh() {
			}
		}(node)
	}

	cleanup := func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			nodes[i].Stop()
		}
		wg.Wait()
	}
	return nodes, net, cleanup, nil
}

func startRaftIntegrationSingleVoter(id string) (*Node, func(), error) {
	node, err := NewNode(Config{ID: id})
	if err != nil {
		return nil, nil, err
	}
	node.Start()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range node.ApplyCh() {
		}
	}()

	cleanup := func() {
		node.Stop()
		<-done
	}
	if err := waitFor(time.Second, func() bool { return node.IsLeader() }); err != nil {
		cleanup()
		return nil, nil, err
	}
	return node, cleanup, nil
}
