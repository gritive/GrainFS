package server

import (
	"github.com/gritive/GrainFS/internal/cache/shardcache"
)

func WithShardCache(c *shardcache.Cache) Option {
	return func(s *Server) {
		s.shardCache = c
	}
}

func WithClusterInfo(ci ClusterInfo) Option {
	return func(s *Server) {
		s.cluster = ci
	}
}

func WithClusterMembership(m ClusterMembership) Option {
	return func(s *Server) {
		s.membership = m
	}
}

func WithBalancerInfo(bi BalancerInfo) Option {
	return func(s *Server) {
		s.balancer = bi
	}
}

func WithJoinCluster(fn JoinClusterFunc) Option {
	return func(s *Server) {
		s.joinCluster = fn
	}
}

func WithExpandPlacement(fn ExpandPlacementFunc) Option {
	return func(s *Server) {
		s.expandPlacement = fn
	}
}

// WithVerifyPerVersionCutover injects the per-version cutover-readiness
// aggregator (S4a). Injected by serveruntime once *cluster.DistributedBackend
// is constructed (bootOwnedGroupsAndEC → boot_phases_srvopts).
func WithVerifyPerVersionCutover(fn VerifyPerVersionCutoverFunc) Option {
	return func(s *Server) {
		s.verifyPerVersionCutoverFn = fn
	}
}

func WithReadIndexer(ri ReadIndexer) Option {
	return func(s *Server) {
		s.readIndexer = ri
	}
}

func WithRaftSnapshotter(rs RaftSnapshotter) Option {
	return func(s *Server) {
		s.raftSnapshots = rs
	}
}
