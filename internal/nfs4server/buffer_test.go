package nfs4server

import (
	"testing"
)

func TestBufferPoolGetPutCycle(t *testing.T) {
	pool := NewBufferPool()

	// 작은 버퍼 테스트
	buf1 := pool.getBuffer(500 * 1024) // 500KB
	if cap(buf1) != bufferSmall {
		t.Fatalf("expected small buffer (32KB), got %d", cap(buf1))
	}
	pool.putBuffer(buf1)

	// 중간 버퍼 테스트
	buf2 := pool.getBuffer(5 * 1024 * 1024) // 5MB
	if cap(buf2) != bufferMedium {
		t.Fatalf("expected medium buffer (256KB), got %d", cap(buf2))
	}
	pool.putBuffer(buf2)

	// 대용량 버퍼 테스트
	buf3 := pool.getBuffer(50 * 1024 * 1024) // 50MB
	if cap(buf3) != bufferLarge {
		t.Fatalf("expected large buffer (1MB), got %d", cap(buf3))
	}
	pool.putBuffer(buf3)
}

func TestBufferPoolConcurrentAccess(t *testing.T) {
	pool := NewBufferPool()
	done := make(chan bool)

	// 10개의 고루틴을 실행하여 get/put 사이클 수행
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				buf := pool.getBuffer(50 * 1024 * 1024)
				// 작업 시뮬레이션
				buf[0] = byte(j)
				pool.putBuffer(buf)
			}
			done <- true
		}()
	}

	// 모든 고루틴이 완료될 때까지 대기
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestBufferPoolPutNil(t *testing.T) {
	pool := NewBufferPool()
	pool.putBuffer(nil) // 패닉이 발생하면 안 됨
}
