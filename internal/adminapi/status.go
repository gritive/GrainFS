package adminapi

// StatusReport is the JSON body returned by GET /v1/status. It aggregates
// cluster topology, IAM, encryption, TLS, proxy, audit, and JWT state
// into a single operator-friendly snapshot.
type StatusReport struct {
	Cluster      ClusterStatus    `json:"cluster"`
	IAM          IAMStatus        `json:"iam"`
	Encryption   EncryptionStatus `json:"encryption"`
	TLS          TLSStatus        `json:"tls"`
	TrustedProxy []string         `json:"trusted_proxy"`
	Audit        AuditStatus      `json:"audit"`
	JWTKeys      JWTStatus        `json:"jwt_keys"`
	Banner       bool             `json:"banner"`
}

// ClusterStatus holds cluster topology (node identity and size).
type ClusterStatus struct {
	NodeID      string `json:"node_id"`
	ClusterSize int    `json:"cluster_size"`
}

// IAMStatus summarises identity and access management state.
type IAMStatus struct {
	SACount int `json:"sa_count"`
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
