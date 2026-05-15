package server

import (
	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

func (s *Server) clusterStatusSnapshot(bucket string) adminapi.Status {
	return buildClusterStatus(s.cluster, s.degradedFlag.Load(), bucket)
}

func (s *Server) clusterHealthSnapshot() Health {
	return buildClusterHealth(s.cluster, s.degradedFlag.Load())
}

func (s *Server) raftHealthSnapshot() map[string]any {
	return buildRaftHealthResponse(s.cluster)
}

func (s *Server) clusterPlacementReport(bucket, key string, limit int) cluster.PlacementReport {
	if !s.routeFeatureAvailable(routeFeatureCluster) {
		return emptyClusterPlacementReport(bucket, key)
	}
	return s.cluster.PlacementReport(bucket, key, limit)
}

func emptyClusterPlacementReport(bucket, key string) cluster.PlacementReport {
	return cluster.PlacementReport{
		DesiredPolicyBasis:  "group_voter_count",
		Bucket:              bucket,
		Key:                 key,
		ActualProfileCounts: map[string]int{},
	}
}
