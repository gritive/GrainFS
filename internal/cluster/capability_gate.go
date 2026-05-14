package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/raft"
)

type CapabilityGate struct {
	mu       sync.RWMutex
	registry *compat.Registry
	ttl      time.Duration
	configID uint64
	config   raft.Configuration
	evidence map[compat.NodeID]compat.Evidence
}

func NewCapabilityGate(registry *compat.Registry, ttl time.Duration) *CapabilityGate {
	if registry == nil {
		registry = compat.DefaultRegistry
	}
	return &CapabilityGate{
		registry: registry,
		ttl:      ttl,
		evidence: make(map[compat.NodeID]compat.Evidence),
	}
}

func (g *CapabilityGate) SetMetaRaftSnapshot(_ uint64, cfg raft.Configuration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.configID = raftConfigurationID(cfg)
	g.config = cfg
}

func (g *CapabilityGate) ReportEvidence(ev compat.Evidence) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.evidence[ev.NodeID] = ev
}

func (g *CapabilityGate) RequireMetaRaftCapability(capability string, op compat.Operation, now time.Time) (compat.GatePlan, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	capDef, ok := g.registry.Lookup(capability)
	if !ok {
		plan := compat.GatePlan{
			Capability: capability,
			Scope:      compat.ScopeMetaRaft,
			Severity:   compat.SeverityHard,
			Operation:  op,
			ConfigID:   g.configID,
			Unknown:    []compat.NodeID{"registry"},
		}
		return plan, compat.Reject(plan)
	}

	plan := compat.GatePlan{
		Capability: capability,
		Scope:      capDef.Scope,
		Severity:   capDef.Severity,
		Operation:  op,
		ConfigID:   g.configID,
	}
	for _, srv := range g.config.Servers {
		nodeID := compat.NodeID(srv.ID)
		plan.Required = append(plan.Required, nodeID)
		ev, ok := g.evidence[nodeID]
		if !ok {
			plan.Unknown = append(plan.Unknown, nodeID)
			continue
		}
		if !ev.Ready || !ev.Capabilities[capability] {
			plan.Missing = append(plan.Missing, nodeID)
			continue
		}
		if now.Sub(ev.LastSeen) > g.ttl {
			plan.Stale = append(plan.Stale, compat.StaleNode{NodeID: nodeID, LastSeen: ev.LastSeen})
		}
	}
	sort.Slice(plan.Required, func(i, j int) bool { return plan.Required[i] < plan.Required[j] })
	sort.Slice(plan.Missing, func(i, j int) bool { return plan.Missing[i] < plan.Missing[j] })
	sort.Slice(plan.Unknown, func(i, j int) bool { return plan.Unknown[i] < plan.Unknown[j] })
	if !plan.Allowed() {
		return plan, compat.Reject(plan)
	}
	return plan, nil
}

func (g *CapabilityGate) ValidatePlanStillCurrent(plan compat.GatePlan) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if plan.ConfigID != g.configID {
		return fmt.Errorf("compat: gate plan config changed from %d to %d", plan.ConfigID, g.configID)
	}
	return nil
}

func raftConfigurationID(cfg raft.Configuration) uint64 {
	servers := append([]raft.Server(nil), cfg.Servers...)
	sort.Slice(servers, func(i, j int) bool {
		if servers[i].ID == servers[j].ID {
			return servers[i].Suffrage < servers[j].Suffrage
		}
		return servers[i].ID < servers[j].ID
	})
	h := fnv.New64a()
	for _, srv := range servers {
		_, _ = fmt.Fprintf(h, "%s\x00%d\x00", srv.ID, srv.Suffrage)
	}
	return h.Sum64()
}
