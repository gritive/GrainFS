// internal/audit/wire.go
package audit

import (
	"encoding/binary"
	"fmt"
	"io"
)

// EncodeS3Batch serializes a slice of S3Events to binary.
// Wire format: [4B count LE][events...]
// Each event: [8B Ts][2B+str NodeID][2B+str RequestID][2B+str SAID][2B+str SourceIP]
//
//	[1B+str Method][2B+str Bucket][2B+str Key]
//	[4B Status][8B BytesIn][8B BytesOut][4B LatencyMs][2B+str ErrClass]
func EncodeS3Batch(events []S3Event) ([]byte, error) {
	buf := make([]byte, 0, len(events)*128)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(events)))
	buf = append(buf, hdr[:]...)
	for _, e := range events {
		buf = appendInt64LE(buf, e.Ts)
		buf = appendString16(buf, e.NodeID)
		buf = appendString16(buf, e.RequestID)
		buf = appendString16(buf, e.SAID)
		buf = appendString16(buf, e.SourceIP)
		buf = appendString8(buf, e.Method)
		buf = appendString16(buf, e.Bucket)
		buf = appendString16(buf, e.Key)
		buf = appendInt32LE(buf, e.Status)
		buf = appendInt64LE(buf, e.BytesIn)
		buf = appendInt64LE(buf, e.BytesOut)
		buf = appendInt32LE(buf, e.LatencyMs)
		buf = appendString16(buf, e.ErrClass)
	}
	return buf, nil
}

const maxDecodeBatchSize = 65536

// DecodeS3Batch decodes bytes produced by EncodeS3Batch into S3Events.
func DecodeS3Batch(data []byte) ([]S3Event, error) {
	r := &byteReader{data: data}
	countBytes, err := r.read(4)
	if err != nil {
		return nil, err
	}
	count := binary.LittleEndian.Uint32(countBytes)
	if count > maxDecodeBatchSize {
		return nil, fmt.Errorf("decode: batch count %d exceeds maximum %d", count, maxDecodeBatchSize)
	}
	out := make([]S3Event, 0, count)
	for i := uint32(0); i < count; i++ {
		var e S3Event
		if e.Ts, err = r.readInt64LE(); err != nil {
			return nil, fmt.Errorf("event %d ts: %w", i, err)
		}
		if e.NodeID, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.RequestID, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.SAID, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.SourceIP, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Method, err = r.readString8(); err != nil {
			return nil, err
		}
		if e.Bucket, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Key, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Status, err = r.readInt32LE(); err != nil {
			return nil, err
		}
		if e.BytesIn, err = r.readInt64LE(); err != nil {
			return nil, err
		}
		if e.BytesOut, err = r.readInt64LE(); err != nil {
			return nil, err
		}
		if e.LatencyMs, err = r.readInt32LE(); err != nil {
			return nil, err
		}
		if e.ErrClass, err = r.readString16(); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func appendInt64LE(buf []byte, v int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	return append(buf, b[:]...)
}

func appendInt32LE(buf []byte, v int32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	return append(buf, b[:]...)
}

func appendString16(buf []byte, s string) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], uint16(len(s)))
	buf = append(buf, b[:]...)
	return append(buf, s...)
}

func appendString8(buf []byte, s string) []byte {
	buf = append(buf, byte(len(s)))
	return append(buf, s...)
}

type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) read(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *byteReader) readInt64LE() (int64, error) {
	b, err := r.read(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (r *byteReader) readInt32LE() (int32, error) {
	b, err := r.read(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b)), nil
}

func (r *byteReader) readString16() (string, error) {
	lenBytes, err := r.read(2)
	if err != nil {
		return "", err
	}
	n := int(binary.LittleEndian.Uint16(lenBytes))
	b, err := r.read(n)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *byteReader) readString8() (string, error) {
	lenByte, err := r.read(1)
	if err != nil {
		return "", err
	}
	b, err := r.read(int(lenByte[0]))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
