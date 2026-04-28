// Package chaos provides fault injection primitives for testing Raft
// safety/liveness properties under partitions, message loss, and node
// restarts. It runs in-memory (no real QUIC) by intercepting the existing
// raft.Node transport callbacks and routing RPCs synchronously to peers'
// public Handle* methods.
//
// Test scenarios live in chaos/scenarios/. Each scenario maps to a real
// Raft incident pattern (split-brain, leader isolation, etc.).
package chaos

// Driver is the test-facing fault injection API. Implementations own the
// node registry and lifecycle.
type Driver interface {
	// PartitionPeer blocks all messages in and out of nodeID until HealPartition.
	PartitionPeer(nodeID string)

	// HealPartition restores message delivery for nodeID.
	HealPartition(nodeID string)

	// DropMessage drops the next n outbound messages from→to. Counter persists
	// until exhausted, then normal delivery resumes.
	DropMessage(from, to string, n int)

	// RestartNode performs Close() on the node, then NewNode + Start with the
	// same Config. Log store state is NOT preserved — in-memory only.
	RestartNode(nodeID string)
}
