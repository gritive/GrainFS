package server

import "github.com/gritive/GrainFS/internal/snapshot"

func (s *Server) SnapMgrForTest() *snapshot.Manager { return s.snapMgr }
