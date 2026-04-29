package nfs4server

import (
	"io"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/pool"
)

const (
	bufferSmall  = 32 * 1024  // 32KB
	bufferMedium = 256 * 1024 // 256KB
	bufferLarge  = 1 * 1024 * 1024 // 1MB
)

// BufferPool은 다양한 크기의 재사용 가능한 버퍼를 관리합니다.
type BufferPool struct {
	small  *pool.Pool[[]byte]
	medium *pool.Pool[[]byte]
	large  *pool.Pool[[]byte]
}

// 전역 버퍼 풀 인스턴스.
var globalBufferPool *BufferPool

func init() {
	globalBufferPool = NewBufferPool()
}

// NewBufferPool은 32KB, 256KB, 1MB 버퍼로 새 버퍼 풀을 생성합니다.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		small:  pool.New(func() []byte { return make([]byte, bufferSmall) }),
		medium: pool.New(func() []byte { return make([]byte, bufferMedium) }),
		large:  pool.New(func() []byte { return make([]byte, bufferLarge) }),
	}
}

// getBuffer는 요청된 크기에 따라 적절한 크기의 버퍼를 반환합니다.
// 작은 요청(<1MB): 32KB 버퍼 반환
// 중간 요청(1-10MB): 256KB 버퍼 반환
// 대용량 요청(>10MB): 1MB 버퍼 반환
func (p *BufferPool) getBuffer(size int64) []byte {
	var buf []byte
	var sizeLabel string

	if size < 1*1024*1024 {
		buf = p.small.Get()
		sizeLabel = "small"
	} else if size < 10*1024*1024 {
		buf = p.medium.Get()
		sizeLabel = "medium"
	} else {
		buf = p.large.Get()
		sizeLabel = "large"
	}

	metrics.NFSv4BufferPoolGets.WithLabelValues(sizeLabel).Inc()

	// 재사용된 버퍼(내용이 0으로 지워짐)인지 새 할당인지 확인
	if len(buf) > 0 && buf[0] != 0 {
		// 재사용된 것으로 추정
		metrics.NFSv4BufferSizeInUse.WithLabelValues(sizeLabel).Set(float64(cap(buf)))
	}

	return buf
}

// putBuffer는 버퍼를 풀에 반환합니다. 용량으로 원래 크기를 판단하여
// 적절한 풀에 반환합니다.
func (p *BufferPool) putBuffer(buf []byte) {
	if buf == nil {
		return
	}

	var sizeLabel string
	switch cap(buf) {
	case bufferSmall:
		sizeLabel = "small"
		p.small.Put(buf)
	case bufferMedium:
		sizeLabel = "medium"
		p.medium.Put(buf)
	case bufferLarge:
		sizeLabel = "large"
		p.large.Put(buf)
	default:
		// 알 수 없는 크기 - 미스로 추적
		metrics.NFSv4BufferPoolMisses.WithLabelValues("unknown").Inc()
		return
	}

	metrics.NFSv4BufferSizeInUse.WithLabelValues(sizeLabel).Set(0)
}

// bufferedCopy는 적응형 버퍼 사이징을 사용하여 src에서 dst로 데이터를 복사합니다.
// 적절한 크기의 청크를 읽어 dst에 씁니다.
// 대용량 파일의 경우 전체 파일을 한 번에 메모리에 로드하지 않으므로
// io.ReadAll보다 효율적입니다.
func bufferedCopy(dst io.Writer, src io.Reader, totalSize int64) (int64, error) {
	buf := globalBufferPool.getBuffer(totalSize)
	defer globalBufferPool.putBuffer(buf)

	var written int64
	for {
		nr, err := src.Read(buf)
		if nr > 0 {
			nw, err := dst.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if err != nil {
				return written, err
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return written, err
		}
	}
	return written, nil
}
