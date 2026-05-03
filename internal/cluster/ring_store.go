package cluster

import (
	"encoding/binary"
	"errors"
	"sync"
)

var ErrRingNotFound = errors.New("ring version not found")

type ringStore struct {
	mu       sync.RWMutex
	current  RingVersion
	rings    map[RingVersion]*Ring
	refCount map[RingVersion]int64
}

func newRingStore() *ringStore {
	return &ringStore{
		rings:    make(map[RingVersion]*Ring),
		refCount: make(map[RingVersion]int64),
	}
}

func (s *ringStore) putRing(r *Ring) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rings[r.Version] = r
	if r.Version > s.current {
		s.current = r.Version
	}
}

func (s *ringStore) GetRing(version RingVersion) (*Ring, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rings[version]
	if !ok {
		return nil, ErrRingNotFound
	}
	return r, nil
}

func (s *ringStore) GetCurrentRing() (*Ring, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current == 0 {
		return nil, ErrRingNotFound
	}
	r, ok := s.rings[s.current]
	if !ok {
		return nil, ErrRingNotFound
	}
	return r, nil
}

func (s *ringStore) incRef(version RingVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refCount[version]++
}

func (s *ringStore) decRef(version RingVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refCount[version] > 0 {
		s.refCount[version]--
	}
}

//nolint:unused // package tests pin ring GC eligibility semantics.
func (s *ringStore) gcEligible(version RingVersion) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.refCount[version] == 0 && s.current > version
}

// encodeRingForDB serializes a Ring for BadgerDB persistence.
// Format: [version:8][vPerNode:4][len:4][token:4,nodeIDLen:2,nodeID:...]...
//
//nolint:unused // package tests pin the on-disk ring encoding contract.
func encodeRingForDB(r *Ring) ([]byte, error) {
	size := 8 + 4 + 4
	for _, vn := range r.VNodes {
		size += 4 + 2 + len(vn.NodeID)
	}
	buf := make([]byte, size)
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], uint64(r.Version))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(r.VPerNode))
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.VNodes)))
	off += 4
	for _, vn := range r.VNodes {
		binary.LittleEndian.PutUint32(buf[off:], vn.Token)
		off += 4
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(vn.NodeID)))
		off += 2
		copy(buf[off:], vn.NodeID)
		off += len(vn.NodeID)
	}
	return buf, nil
}

// decodeRingFromDB deserializes a Ring from BadgerDB bytes.
//
//nolint:unused // package tests pin the on-disk ring encoding contract.
func decodeRingFromDB(data []byte) (*Ring, error) {
	if len(data) < 16 {
		return nil, errors.New("ring data too short")
	}
	off := 0
	version := RingVersion(binary.LittleEndian.Uint64(data[off:]))
	off += 8
	vPerNode := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	count := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	vnodes := make([]VirtualNode, 0, count)
	for i := 0; i < count; i++ {
		if off+6 > len(data) {
			return nil, errors.New("ring data truncated")
		}
		token := binary.LittleEndian.Uint32(data[off:])
		off += 4
		idLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+idLen > len(data) {
			return nil, errors.New("ring data truncated at nodeID")
		}
		nodeID := string(data[off : off+idLen])
		off += idLen
		vnodes = append(vnodes, VirtualNode{Token: token, NodeID: nodeID})
	}
	return &Ring{Version: version, VNodes: vnodes, VPerNode: vPerNode}, nil
}
