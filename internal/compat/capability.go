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
)

const (
	CapabilityMigrationCutoverV1 = "migration_cutover_v1"
	CapabilityNfsExportCreateV1  = "nfs_export_create_v1"
	CapabilityMultipartListingV1 = "multipart_listing_v1"
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
	byName map[string]Capability
}

var DefaultRegistry = mustRegistry([]Capability{
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
})

func mustRegistry(caps []Capability) *Registry {
	reg, err := NewRegistry(caps)
	if err != nil {
		panic(err)
	}
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
