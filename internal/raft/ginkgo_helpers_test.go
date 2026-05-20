package raft

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func startMembershipClusterGinkgo(ids []string) *membershipFixture {
	Expect(ids).NotTo(BeEmpty())
	fix := &membershipFixture{net: newMemNetwork()}

	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		electionTimeout := slowElectionTimeout
		if i == 0 {
			electionTimeout = fastElectionTimeout
		}
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: testHeartbeat,
		})
		Expect(err).NotTo(HaveOccurred())
		fix.nodes = append(fix.nodes, n)
	}

	for _, n := range fix.nodes {
		n.SetTransport(fix.net.Register(n.cfg.ID, n))
	}
	for _, n := range fix.nodes {
		startGinkgoMembershipNode(fix, n)
	}
	return fix
}

func (f *membershipFixture) addNodeGinkgo(id string, seedPeers []string, electionTimeout time.Duration) *Node {
	if electionTimeout == 0 {
		electionTimeout = slowElectionTimeout
	}
	n, err := NewNode(Config{
		ID:               id,
		Peers:            seedPeers,
		ElectionTimeout:  electionTimeout,
		HeartbeatTimeout: testHeartbeat,
	})
	Expect(err).NotTo(HaveOccurred())
	n.SetTransport(f.net.Register(id, n))
	startGinkgoMembershipNode(f, n)
	f.nodes = append(f.nodes, n)
	return n
}

func startGinkgoMembershipNode(f *membershipFixture, n *Node) {
	n.Start()
	DeferCleanup(n.Stop)
	f.wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for range n.ApplyCh() {
		}
	}(&f.wg)
}
