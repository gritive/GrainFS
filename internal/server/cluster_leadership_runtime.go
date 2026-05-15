package server

import (
	"errors"

	"github.com/gritive/GrainFS/internal/raft"
)

var (
	errClusterModeNotConfigured     = errors.New("cluster mode not configured")
	errClusterTransferNotSupported  = errors.New("cluster adapter does not support transfer-leader")
	errClusterTransferSingleNode    = errors.New("single-node mode: no peers to transfer to")
	errClusterTransferInternalState = errors.New("cluster transfer-leader failed")
)

func (s *Server) transferClusterLeadership() (TransferLeaderResult, error) {
	if !s.routeFeatureAvailable(routeFeatureCluster) {
		return TransferLeaderResult{}, errClusterModeNotConfigured
	}
	tl, ok := s.cluster.(clusterTransferLeader)
	if !ok {
		return TransferLeaderResult{}, errClusterTransferNotSupported
	}
	if !tl.IsLeader() {
		return TransferLeaderResult{}, clusterNotLeaderError{leaderID: clusterLeaderID(s.cluster)}
	}
	result := TransferLeaderResult{
		OldLeader: clusterNodeID(s.cluster),
		Term:      clusterTerm(s.cluster),
	}
	if err := tl.TransferLeadership(); err != nil {
		switch {
		case errors.Is(err, raft.ErrNoPeers):
			return TransferLeaderResult{}, errClusterTransferSingleNode
		case errors.Is(err, raft.ErrNotLeader):
			return TransferLeaderResult{}, clusterNotLeaderError{
				leaderID: clusterLeaderID(s.cluster),
				retry:    true,
			}
		default:
			return TransferLeaderResult{}, errors.Join(errClusterTransferInternalState, err)
		}
	}
	return result, nil
}
