package admin

import "context"

// ListClusterPeers returns the peer-health snapshot for the cluster. When
// PeerHealth is not wired (single-node deploy, harness without
// SetShardService), the response carries an empty list rather than 404 —
// operators see "0 peers" instead of an error.
func ListClusterPeers(ctx context.Context, d *Deps) (ListClusterPeersResp, error) {
	if d == nil || d.PeerHealth == nil {
		return ListClusterPeersResp{Peers: []ClusterPeerInfo{}}, nil
	}
	peers := d.PeerHealth.Snapshot()
	if peers == nil {
		peers = []ClusterPeerInfo{}
	}
	return ListClusterPeersResp{Peers: peers}, nil
}
