package server

import (
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
)

func WithBlockCache(c *blockcache.Cache) Option {
	return func(s *Server) {
		s.blockCache = c
	}
}

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
