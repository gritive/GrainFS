package adminapi

// StatusReport is the JSON body returned by GET /v1/status. It aggregates
// cluster topology, phase, IAM, encryption, TLS, proxy, audit, and JWT state
// into a single operator-friendly snapshot.
type StatusReport struct {
	Cluster    ClusterStatus    `json:"cluster"`
	IAM        IAMStatus        `json:"iam"`
	Encryption EncryptionStatus `json:"encryption"`
	TLS        TLSStatus        `json:"tls"`
	Proxy      ProxyStatus      `json:"proxy,omitempty"`
	Audit      AuditStatus      `json:"audit"`
	JWT        JWTStatus        `json:"jwt"`
}

// ClusterStatus holds cluster topology and the derived readiness phase.
//
// Phase derivation (server-side):
//
//	0 = sa_count==0 && cluster_size==1  (single-node, no IAM bootstrap)
//	1 = sa_count==0 && cluster_size>1   (cluster, no IAM bootstrap)
//	2 = sa_count>=1 && !tls.cert_present (IAM bootstrapped, TLS not configured)
//	3 = sa_count>=1 && tls.cert_present  (production-ready)
type ClusterStatus struct {
	NodeID      string `json:"node_id"`
	ClusterSize int    `json:"cluster_size"`
	Phase       int    `json:"phase"`
}

// IAMStatus summarises identity and access management state.
type IAMStatus struct {
	SACount int  `json:"sa_count"`
	Banner  bool `json:"banner"` // true when iam.anon-enabled = true (Phase 0 banner)
}

// EncryptionStatus describes at-rest encryption posture.
type EncryptionStatus struct {
	Enabled bool   `json:"enabled"`
	DEKGen  uint32 `json:"dek_gen"`
}

// TLSStatus describes TLS certificate presence.
type TLSStatus struct {
	CertPresent bool `json:"cert_present"`
}

// ProxyStatus describes the trusted-proxy CIDR configuration.
// Omitted from JSON when TrustedCIDR is empty (omitempty on the parent).
type ProxyStatus struct {
	TrustedCIDR string `json:"trusted_cidr,omitempty"`
}

// AuditStatus describes audit configuration.
type AuditStatus struct {
	DenyOnly bool `json:"deny_only"`
}

// JWTStatus describes the in-memory JWT signing key set.
// PreviousKID is the previously-current KID kept for token verification
// during key rotation; it is NOT a "next" key.
type JWTStatus struct {
	CurrentKID  string `json:"current_kid,omitempty"`
	PreviousKID string `json:"previous_kid,omitempty"`
}
