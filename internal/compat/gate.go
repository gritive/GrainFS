package compat

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrCapabilityRejected = errors.New("compat: capability rejected")

type NodeID string

type StaleNode struct {
	NodeID   NodeID
	LastSeen time.Time
}

type Evidence struct {
	NodeID       NodeID
	Capabilities map[string]bool
	LastSeen     time.Time
	Ready        bool
}

type GatePlan struct {
	Capability string
	Scope      Scope
	Severity   Severity
	Operation  Operation
	ConfigID   uint64
	Required   []NodeID
	Missing    []NodeID
	Unknown    []NodeID
	Stale      []StaleNode
}

func (p GatePlan) Allowed() bool {
	return len(p.Missing) == 0 && len(p.Unknown) == 0 && len(p.Stale) == 0
}

type GateRejectError struct {
	Plan GatePlan
}

func (e *GateRejectError) Error() string {
	if e == nil {
		return "<nil>"
	}
	parts := []string{
		fmt.Sprintf("capability %s rejected", e.Plan.Capability),
		fmt.Sprintf("scope=%s", e.Plan.Scope),
		fmt.Sprintf("operation=%s", e.Plan.Operation),
	}
	if len(e.Plan.Missing) > 0 {
		parts = append(parts, fmt.Sprintf("missing=%s", nodeList(e.Plan.Missing)))
	}
	if len(e.Plan.Unknown) > 0 {
		parts = append(parts, fmt.Sprintf("unknown=%s", nodeList(e.Plan.Unknown)))
	}
	if len(e.Plan.Stale) > 0 {
		parts = append(parts, fmt.Sprintf("stale=%s", staleList(e.Plan.Stale)))
	}
	return strings.Join(parts, ": ")
}

func (e *GateRejectError) Unwrap() error { return ErrCapabilityRejected }

func (e *GateRejectError) PublicMessage() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("capability %s rejected for operation %s; finish the rolling upgrade before retrying", e.Plan.Capability, e.Plan.Operation)
}

func Reject(plan GatePlan) error {
	RecordReject(plan, false)
	return &GateRejectError{Plan: plan}
}

func nodeList(nodes []NodeID) string {
	vals := make([]string, 0, len(nodes))
	for _, n := range nodes {
		vals = append(vals, string(n))
	}
	return "[" + strings.Join(vals, ",") + "]"
}

func staleList(nodes []StaleNode) string {
	vals := make([]string, 0, len(nodes))
	for _, n := range nodes {
		vals = append(vals, fmt.Sprintf("%s last_seen=%s", n.NodeID, n.LastSeen.UTC().Format(time.RFC3339)))
	}
	return "[" + strings.Join(vals, ",") + "]"
}
