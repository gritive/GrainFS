package s3auth

import (
	"bytes"
	"fmt"
	"strconv"
)

// DecodeAWSChunkedBody decodes an aws-chunked encoded body.
// Format: hex_size;chunk-signature=...\r\ndata\r\n...0;chunk-signature=...\r\n\r\n
func DecodeAWSChunkedBody(data []byte) ([]byte, error) {
	readAt := 0
	writeAt := 0

	for {
		line, next, err := readLineSlice(data, readAt)
		if err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}
		readAt = next

		sizeStr := line
		if idx := bytes.IndexByte(line, ';'); idx >= 0 {
			sizeStr = line[:idx]
		}

		size, err := strconv.ParseInt(string(bytes.TrimSpace(sizeStr)), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("parse chunk size %q: %w", sizeStr, err)
		}
		if size == 0 {
			break
		}
		if size < 0 || size > int64(len(data)-readAt) {
			return nil, fmt.Errorf("read chunk data: unexpected EOF")
		}

		chunkEnd := readAt + int(size)
		copy(data[writeAt:], data[readAt:chunkEnd])
		writeAt += int(size)
		readAt = chunkEnd

		next, err = skipLineEnding(data, readAt)
		if err != nil {
			return nil, fmt.Errorf("read chunk trailer: %w", err)
		}
		readAt = next
	}

	return data[:writeAt], nil
}

func readLineSlice(data []byte, start int) ([]byte, int, error) {
	if start >= len(data) {
		return nil, start, fmt.Errorf("EOF")
	}
	rel := bytes.IndexByte(data[start:], '\n')
	if rel < 0 {
		return nil, start, fmt.Errorf("EOF")
	}
	end := start + rel
	line := data[start:end]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, end + 1, nil
}

func skipLineEnding(data []byte, start int) (int, error) {
	if start >= len(data) {
		return start, fmt.Errorf("unexpected EOF")
	}
	if data[start] == '\n' {
		return start + 1, nil
	}
	if data[start] == '\r' {
		if start+1 >= len(data) || data[start+1] != '\n' {
			return start, fmt.Errorf("invalid CRLF")
		}
		return start + 2, nil
	}
	return start, fmt.Errorf("invalid line ending")
}
