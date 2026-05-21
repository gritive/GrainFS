package cluster

// RingVersion은 링 스냅샷의 단조 증가 버전 번호다.
// Task 8에서 FlatBuffers ring_version 필드 제거 시 함께 삭제 예정.
type RingVersion uint64

// VirtualNode는 SetRingCmd 역직렬화 경로에서 사용되는 레거시 타입이다.
// Task 8에서 FlatBuffers SetRingCmd 스키마 제거 시 함께 삭제 예정.
type VirtualNode struct {
	Token  uint32
	NodeID string
}
