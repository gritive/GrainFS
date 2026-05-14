// internal/audit/wire.go
package audit

import (
	"encoding/binary"
	"fmt"
	"io"
)

// EncodeS3Batch serializes a slice of S3Events to binary.
// Wire format: [4B count LE][events...]
// Each event: [8B Ts][2B+str EventID][2B+str NodeID][2B+str RequestID][2B+str SAID][2B+str SourceIP]
//
//	[2B+str UserAgent][1B+str Method][2B+str Operation][2B+str Bucket][2B+str Key][2B+str Subresource]
//	[4B Status][2B+str AuthStatus][8B BytesIn][8B BytesOut][4B LatencyMs]
//	[2B+str ErrClass][2B+str ErrReason][2B+str VersionID][2B+str UploadID]
//	[2B+str CopySourceBucket][2B+str CopySourceKey]
func EncodeS3Batch(events []S3Event) ([]byte, error) {
	buf := make([]byte, 0, len(events)*128)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(events)))
	buf = append(buf, hdr[:]...)
	for _, e := range events {
		buf = appendInt64LE(buf, e.Ts)
		var err error
		if buf, err = appendString16(buf, "event_id", e.EventID); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "node_id", e.NodeID); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "request_id", e.RequestID); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "sa_id", e.SAID); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "source_ip", e.SourceIP); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "user_agent", e.UserAgent); err != nil {
			return nil, err
		}
		if buf, err = appendString8(buf, "method", e.Method); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "operation", e.Operation); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "bucket", e.Bucket); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "key", e.Key); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "subresource", e.Subresource); err != nil {
			return nil, err
		}
		buf = appendInt32LE(buf, e.Status)
		if buf, err = appendString16(buf, "auth_status", e.AuthStatus); err != nil {
			return nil, err
		}
		buf = appendInt64LE(buf, e.BytesIn)
		buf = appendInt64LE(buf, e.BytesOut)
		buf = appendInt32LE(buf, e.LatencyMs)
		if buf, err = appendString16(buf, "err_class", e.ErrClass); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "err_reason", e.ErrReason); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "version_id", e.VersionID); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "upload_id", e.UploadID); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "copy_source_bucket", e.CopySourceBucket); err != nil {
			return nil, err
		}
		if buf, err = appendString16(buf, "copy_source_key", e.CopySourceKey); err != nil {
			return nil, err
		}
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
		if e.EventID, err = r.readString16(); err != nil {
			return nil, err
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
		if e.UserAgent, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Method, err = r.readString8(); err != nil {
			return nil, err
		}
		if e.Operation, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Bucket, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Key, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Subresource, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.Status, err = r.readInt32LE(); err != nil {
			return nil, err
		}
		if e.AuthStatus, err = r.readString16(); err != nil {
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
		if e.ErrReason, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.VersionID, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.UploadID, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.CopySourceBucket, err = r.readString16(); err != nil {
			return nil, err
		}
		if e.CopySourceKey, err = r.readString16(); err != nil {
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

func appendString16(buf []byte, field, s string) ([]byte, error) {
	if len(s) > 0xffff {
		return nil, fmt.Errorf("%s length %d exceeds maximum 65535", field, len(s))
	}
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], uint16(len(s)))
	buf = append(buf, b[:]...)
	return append(buf, s...), nil
}

func appendString8(buf []byte, field, s string) ([]byte, error) {
	if len(s) > 0xff {
		return nil, fmt.Errorf("%s length %d exceeds maximum 255", field, len(s))
	}
	buf = append(buf, byte(len(s)))
	return append(buf, s...), nil
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
