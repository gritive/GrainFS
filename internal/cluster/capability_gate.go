package cluster

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

type CapabilityGate struct {
	mu       sync.RWMutex
	registry *compat.Registry
	ttl      time.Duration
	configID uint64
	config   raft.Configuration
	evidence map[compat.NodeID]compat.Evidence

	// direct-RPC path (meta_raft scope only). nil = direct probing disabled.
	directCfg   *CapabilityGateDirectConfig
	directCache *directCapabilityCacheEntry // guarded by mu
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

// WithDirectProbe wires the direct-RPC capability probing path used by Allow.
// Must be called before the first Allow call; safe to call at any time (guarded by mu).
func (g *CapabilityGate) WithDirectProbe(clusterID []byte, kekStore *encrypt.KEKStore, dialer capabilityProbeDialer) *CapabilityGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.directCfg = &CapabilityGateDirectConfig{
		dialer:    dialer,
		clusterID: append([]byte(nil), clusterID...),
		kekStore:  kekStore,
	}
	return g
}

// Allow checks whether all current meta-raft voters advertise the capabilities
// required for op via the direct signed-assertion RPC path. Results are cached
// by (op, voter_config_hash, active_kek_version) — the cache automatically
// invalidates on voter config change or KEK rotation.
//
// Gossip capabilities (RequireMetaRaftCapability) are diagnostic-only —
// capability gates use direct RPC via Allow.
//
// Returns (plan, nil) if all voters support op; (plan, error) otherwise.
func (g *CapabilityGate) Allow(ctx context.Context, op compat.Operation) (compat.GatePlan, error) {
	caps, ok := g.registry.RequiredCapabilitiesForOperation(op)
	if !ok {
		// No gate defined for this operation — allowed.
		return compat.GatePlan{Operation: op}, nil
	}

	g.mu.RLock()
	cfg := g.directCfg
	configID := g.configID
	config := g.config
	cache := g.directCache
	g.mu.RUnlock()

	if cfg == nil {
		// Direct probe not wired — fall back to gossip evidence.
		return g.RequireMetaRaftCapability(caps[0], op, time.Now())
	}

	activeKEKVer := cfg.kekStore.ActiveVersion()
	cacheKey := directCapabilityCacheKey{
		op:           op,
		configID:     configID,
		activeKEKVer: activeKEKVer,
	}

	// Return cached result if the key is unchanged.
	if cache != nil && cache.key == cacheKey {
		return cache.plan, cache.err
	}

	// Probe each voter.
	plan := compat.GatePlan{
		Capability: caps[0],
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  op,
		ConfigID:   configID,
	}
	for _, srv := range config.Servers {
		nodeID := compat.NodeID(srv.ID)
		plan.Required = append(plan.Required, nodeID)
	}
	sort.Slice(plan.Required, func(i, j int) bool { return plan.Required[i] < plan.Required[j] })

	for _, srv := range config.Servers {
		peerResp, err := GetCapabilities(ctx, srv.ID, cfg.clusterID, cfg.kekStore, cfg.dialer)
		if err != nil {
			plan.Unknown = append(plan.Unknown, compat.NodeID(srv.ID))
			continue
		}
		// Check each required capability.
		capSet := make(map[string]bool, len(peerResp.Capabilities))
		for _, c := range peerResp.Capabilities {
			capSet[c] = true
		}
		for _, cap := range caps {
			if !capSet[cap] {
				plan.Missing = append(plan.Missing, compat.NodeID(srv.ID))
				break
			}
		}
	}
	sort.Slice(plan.Missing, func(i, j int) bool { return plan.Missing[i] < plan.Missing[j] })
	sort.Slice(plan.Unknown, func(i, j int) bool { return plan.Unknown[i] < plan.Unknown[j] })

	var planErr error
	if !plan.Allowed() {
		planErr = compat.Reject(plan)
	}

	// Store result in cache.
	g.mu.Lock()
	g.directCache = &directCapabilityCacheEntry{key: cacheKey, plan: plan, err: planErr}
	g.mu.Unlock()

	return plan, planErr
}

func (g *CapabilityGate) SetMetaRaftSnapshot(_ uint64, cfg raft.Configuration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.configID = raftConfigurationID(cfg)
	g.config = cfg
}

func (g *CapabilityGate) SetTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ttl = ttl
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

func (g *CapabilityGate) RequirePeerTransportCapability(capability string, op compat.Operation, peers []string, now time.Time) (compat.GatePlan, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	capDef, ok := g.registry.Lookup(capability)
	if !ok {
		plan := compat.GatePlan{
			Capability: capability,
			Scope:      compat.ScopePeerTransport,
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
	for _, peer := range peers {
		nodeID := compat.NodeID(peer)
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

// EvidenceSnapshot returns a copy of every node's currently-known capability
// evidence as `peer → capability → ready`. Read-only — callers may keep the
// result across gate mutations. Used by admin /v1/cluster/capabilities to let
// operators (and the bench warmup probe) wait until every node has gossiped
// its support for a given capability before sending traffic that the gate
// would otherwise reject as "rolling upgrade".
func (g *CapabilityGate) EvidenceSnapshot() map[string]map[string]bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[string]map[string]bool, len(g.evidence))
	for nodeID, ev := range g.evidence {
		caps := make(map[string]bool, len(ev.Capabilities))
		for capability, ready := range ev.Capabilities {
			caps[capability] = ready && ev.Ready
		}
		out[string(nodeID)] = caps
	}
	return out
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
