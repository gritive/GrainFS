package compat

import "fmt"

type Scope string

const (
	ScopeMetaRaft      Scope = "meta_raft"
	ScopeDataGroup     Scope = "data_group"
	ScopePeerTransport Scope = "peer_transport"
	ScopeLocal         Scope = "local"
)

type Severity string

const (
	SeverityHard Severity = "hard"
	SeveritySoft Severity = "soft"
)

type Operation string

const (
	OperationMigrationCutover      Operation = "migration_cutover"
	OperationNfsExportCreate       Operation = "nfs_export_create"
	OperationCreateMultipartUpload Operation = "create_multipart_upload"
	OperationListMultipartUploads  Operation = "list_multipart_uploads"
	OperationListParts             Operation = "list_parts"
	OperationKEKRotate             Operation = "kek_rotate"
	OperationKEKRetire             Operation = "kek_retire"
	OperationKEKPrune              Operation = "kek_prune"
	OperationKEKLeaseSnapshot      Operation = "kek_lease_snapshot"
	OperationKEKStatusQuery        Operation = "kek_status_query"
	OperationDEKRotate             Operation = "dek_rotate"
)

const (
	CapabilityMigrationCutoverV1 = "migration_cutover_v1"
	CapabilityNfsExportCreateV1  = "nfs_export_create_v1"
	CapabilityMultipartListingV1 = "multipart_listing_v1"
	CapabilityKEKEnvelopeV1      = "kek_envelope_v1"
	CapabilityDEKReplicatedV1    = "dek_replicated_v1"
)

type Capability struct {
	Name              string
	Scope             Scope
	Severity          Severity
	IntroducedVersion string
	Description       string
	Semantics         string
}

type Registry struct {
	byName        map[string]Capability
	operationCaps map[Operation][]string
}

var DefaultRegistry = mustRegistryWithOps(
	[]Capability{
		{
			Name:              CapabilityMigrationCutoverV1,
			Scope:             ScopeMetaRaft,
			Severity:          SeverityHard,
			IntroducedVersion: "0.0.193.0",
			Description:       "Allows bucket-upstream cutover status and migration cutover persisted semantics.",
			Semantics:         "Nodes must apply and replay bucket-upstream status semantics and reject unsupported payloads.",
		},
		{
			Name:              CapabilityNfsExportCreateV1,
			Scope:             ScopeMetaRaft,
			Severity:          SeverityHard,
			IntroducedVersion: "0.0.194.0",
			Description:       "Allows create-only NFS export registry commands.",
			Semantics:         "Nodes must apply NfsExportCreate as an atomic create-only registry operation instead of ignoring the command.",
		},
		{
			Name:              CapabilityMultipartListingV1,
			Scope:             ScopePeerTransport,
			Severity:          SeverityHard,
			IntroducedVersion: "0.0.213.0",
			Description:       "Allows clustered multipart listing metadata and remote ListParts forwarding.",
			Semantics:         "Nodes must persist bucket/key multipart metadata and understand the ListParts forward opcode.",
		},
		{
			Name:              CapabilityKEKEnvelopeV1,
			Scope:             ScopeMetaRaft,
			Severity:          SeverityHard,
			IntroducedVersion: "0.0.344.0",
			Description:       "Cluster-wide KEK rotation lifecycle: rotate/retire/prune raft commands, lease attestation, and capability-gated admin triggers.",
			Semantics:         "All voters must understand MetaKEKRotate / MetaKEKRetire / MetaKEKPrune commands and the kek_status FSM state to participate in cluster KEK lifecycle.",
		},
		{
			Name:              CapabilityDEKReplicatedV1,
			Scope:             ScopeMetaRaft,
			Severity:          SeverityHard,
			IntroducedVersion: "0.0.347.0",
			Description:       "Cluster-wide DEK replication: leader-driven DEK bootstrap and rotation committed via Raft so all voters share the active DEK.",
			Semantics:         "All voters must understand MetaDEKReplicatedRotate commands and apply the new DEK atomically to the keystore to participate in cluster DEK lifecycle.",
		},
	},
	map[Operation][]string{
		OperationKEKRotate:        {CapabilityKEKEnvelopeV1},
		OperationKEKRetire:        {CapabilityKEKEnvelopeV1},
		OperationKEKPrune:         {CapabilityKEKEnvelopeV1},
		OperationKEKLeaseSnapshot: {CapabilityKEKEnvelopeV1},
		OperationKEKStatusQuery:   {CapabilityKEKEnvelopeV1},
		OperationDEKRotate:        {CapabilityDEKReplicatedV1},
	},
)

func mustRegistryWithOps(caps []Capability, ops map[Operation][]string) *Registry {
	reg, err := NewRegistry(caps)
	if err != nil {
		panic(err)
	}
	reg.operationCaps = ops
	return reg
}

func NewRegistry(caps []Capability) (*Registry, error) {
	reg := &Registry{byName: make(map[string]Capability, len(caps))}
	for _, cap := range caps {
		if cap.Name == "" {
			return nil, fmt.Errorf("compat: capability name required")
		}
		if !validScope(cap.Scope) {
			return nil, fmt.Errorf("compat: invalid scope %q for %s", cap.Scope, cap.Name)
		}
		if !validSeverity(cap.Severity) {
			return nil, fmt.Errorf("compat: invalid severity %q for %s", cap.Severity, cap.Name)
		}
		if _, exists := reg.byName[cap.Name]; exists {
			return nil, fmt.Errorf("compat: duplicate capability %q", cap.Name)
		}
		reg.byName[cap.Name] = cap
	}
	return reg, nil
}

func (r *Registry) Lookup(name string) (Capability, bool) {
	if r == nil {
		return Capability{}, false
	}
	cap, ok := r.byName[name]
	return cap, ok
}

// RequiredCapabilitiesForOperation returns the capability names required to
// perform op, or (nil, false) if op is not gated. The returned slice is owned
// by the registry; callers MUST NOT modify it.
func (r *Registry) RequiredCapabilitiesForOperation(op Operation) ([]string, bool) {
	if r == nil || r.operationCaps == nil {
		return nil, false
	}
	caps, ok := r.operationCaps[op]
	return caps, ok
}

func validScope(scope Scope) bool {
	switch scope {
	case ScopeMetaRaft, ScopeDataGroup, ScopePeerTransport, ScopeLocal:
		return true
	default:
		return false
	}
}

func validSeverity(severity Severity) bool {
	switch severity {
	case SeverityHard, SeveritySoft:
		return true
	default:
		return false
	}
}
