---
name: lock-free apply notify 패턴
description: sync.Cond 대신 generation channel + atomic.Uint64로 apply 알림 구현하는 패턴
type: feedback
---

sync.Cond + inner goroutine 패턴은 context 취소 시 goroutine 누수가 발생하기 쉽다.  
apply 완료 대기는 "generation channel" + atomic 패턴으로 대체한다.

**Why:** meta_raft.go에서 waitApplied가 sync.Cond.Wait()에서 영구 블록되는 버그가 발생했다. 유저 피드백: mutex 사용 전 lock-free 대안 먼저 검토하라.

**How to apply:** "entry 적용 완료까지 대기" 패턴이 필요할 때:

```go
// 구조체
lastApplied   atomic.Uint64
applyNotifyMu sync.Mutex
applyNotify   chan struct{} // 매 entry 적용 시 close → 새 채널로 교체

// 대기 (goroutine 없음, ctx cancel 시 즉시 반환)
func (m *X) waitApplied(ctx context.Context, idx uint64) error {
    for {
        if m.lastApplied.Load() >= idx { return nil }
        m.applyNotifyMu.Lock()
        ch := m.applyNotify
        m.applyNotifyMu.Unlock()
        select {
        case <-ch:      // 새 entry 적용됨; 재확인
        case <-ctx.Done(): return ctx.Err()
        case <-m.done:  return errors.New("stopped")
        }
    }
}

// 알림 (apply loop에서)
m.lastApplied.Store(entry.Index)
m.applyNotifyMu.Lock()
old := m.applyNotify
m.applyNotify = make(chan struct{})
m.applyNotifyMu.Unlock()
close(old)
```

장점:
- goroutine 없음 → leak 불가
- 읽기 경로 lock-free (atomic.Load)
- ctx cancel 시 즉각 반환
- shutdown 플래그 불필요
