package serveruntime

import (
	"context"
	"os"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// StatusAdapter implements admin.StatusService. It gathers live state from the
// boot fields and returns a single adminapi.StatusReport.
type StatusAdapter struct {
	nodeID      string
	dataDir     string
	peerHealth  admin.PeerHealthAPI
	iamAdminAPI *iam.AdminAPI
	dekKeeper   *encrypt.DEKKeeper
	metaRaft    *cluster.MetaRaft
	cfgStore    *config.Store
}

var _ admin.StatusService = (*StatusAdapter)(nil)

// NewStatusAdapter constructs a StatusAdapter from boot state fields.
func NewStatusAdapter(
	nodeID string,
	dataDir string,
	peerHealth admin.PeerHealthAPI,
	iamAdminAPI *iam.AdminAPI,
	dekKeeper *encrypt.DEKKeeper,
	metaRaft *cluster.MetaRaft,
	cfgStore *config.Store,
) *StatusAdapter {
	return &StatusAdapter{
		nodeID:      nodeID,
		dataDir:     dataDir,
		peerHealth:  peerHealth,
		iamAdminAPI: iamAdminAPI,
		dekKeeper:   dekKeeper,
		metaRaft:    metaRaft,
		cfgStore:    cfgStore,
	}
}

// Report gathers current state and returns a StatusReport.
func (a *StatusAdapter) Report() adminapi.StatusReport {
	// Cluster size: self + peers.
	peers := a.peerHealth.Snapshot()
	clusterSize := len(peers) + 1

	// SA count.
	saCount := 0
	if a.iamAdminAPI != nil {
		if items, err := a.iamAdminAPI.ListSA(context.Background()); err == nil {
			saCount = len(items)
		}
	}

	// TLS cert present.
	tlsCertPresent := false
	if a.dataDir != "" {
		certPath := nodeconfig.New(a.dataDir).TLSCertPath()
		if _, err := os.Stat(certPath); err == nil {
			tlsCertPresent = true
		}
	}

	// DEK encryption state.
	encEnabled := a.dekKeeper != nil
	var dekGen uint32
	if a.dekKeeper != nil {
		dekGen, _ = a.dekKeeper.Active()
	}

	// Banner = iam.anon-enabled (BoolSpec, default true).
	banner := false
	if a.cfgStore != nil {
		banner, _ = a.cfgStore.GetBool("iam.anon-enabled")
	}

	// Audit deny-only (BoolSpec, default false).
	auditDenyOnly := false
	if a.cfgStore != nil {
		auditDenyOnly, _ = a.cfgStore.GetBool("audit.deny-only")
	}

	// Trusted-proxy CIDRs (comma-separated string → []string).
	var trustedProxy []string
	if a.cfgStore != nil {
		if v, ok := a.cfgStore.GetString("trusted-proxy.cidr"); ok && v != "" {
			for _, cidr := range strings.Split(v, ",") {
				if s := strings.TrimSpace(cidr); s != "" {
					trustedProxy = append(trustedProxy, s)
				}
			}
		}
	}

	// JWT key IDs.
	var currentKID, previousKID string
	if a.metaRaft != nil {
		if fsm := a.metaRaft.FSM(); fsm != nil {
			if ks := fsm.JWTKeySet(); ks != nil {
				currentKID = ks.CurrentKID()
				previousKID = ks.PreviousKID()
			}
		}
	}

	phase := derivePhase(saCount, clusterSize, tlsCertPresent)

	return adminapi.StatusReport{
		Cluster: adminapi.ClusterStatus{
			NodeID:      a.nodeID,
			ClusterSize: clusterSize,
		},
		Phase: phase,
		IAM: adminapi.IAMStatus{
			SACount: saCount,
		},
		Encryption: adminapi.EncryptionStatus{
			Enabled: encEnabled,
			DEKGen:  dekGen,
		},
		TLS: adminapi.TLSStatus{
			CertPresent: tlsCertPresent,
		},
		TrustedProxy: trustedProxy,
		Audit: adminapi.AuditStatus{
			DenyOnly: auditDenyOnly,
		},
		JWTKeys: adminapi.JWTStatus{
			CurrentKID:  currentKID,
			PreviousKID: previousKID,
		},
		Banner: banner,
	}
}

// derivePhase maps IAM bootstrap state, cluster size, and TLS cert presence
// to a readiness phase number.
//
//	Phase 0: sa_count==0 && cluster_size==1  (single-node, no IAM bootstrap)
//	Phase 1: sa_count==0 && cluster_size>1   (cluster, no IAM bootstrap)
//	Phase 2: sa_count>=1 && !tls.cert_present (IAM bootstrapped, TLS not configured)
//	Phase 3: sa_count>=1 && tls.cert_present  (production-ready)
func derivePhase(saCount, clusterSize int, tlsCertPresent bool) int {
	switch {
	case saCount == 0 && clusterSize == 1:
		return 0
	case saCount == 0 && clusterSize > 1:
		return 1
	case saCount >= 1 && !tlsCertPresent:
		return 2
	default:
		return 3
	}
}
