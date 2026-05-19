package s3auth

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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

func NewAWSChunkedReader(r io.Reader) io.Reader {
	return &awsChunkedReader{br: bufio.NewReader(r)}
}

type awsChunkedReader struct {
	br       *bufio.Reader
	remain   int64
	done     bool
	needCRLF bool
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.remain == 0 {
		if r.done {
			return 0, io.EOF
		}
		if r.needCRLF {
			if err := r.consumeChunkCRLF(); err != nil {
				return 0, err
			}
			r.needCRLF = false
		}
		size, err := r.readChunkSize()
		if err != nil {
			return 0, err
		}
		if size == 0 {
			if err := r.consumeTrailers(); err != nil {
				return 0, err
			}
			r.done = true
			return 0, io.EOF
		}
		r.remain = size
	}
	if int64(len(p)) > r.remain {
		p = p[:r.remain]
	}
	n, err := io.ReadFull(r.br, p)
	r.remain -= int64(n)
	if err != nil {
		return n, fmt.Errorf("read chunk data: %w", err)
	}
	if r.remain == 0 {
		r.needCRLF = true
	}
	return n, nil
}

func (r *awsChunkedReader) readChunkSize() (int64, error) {
	line, err := r.br.ReadBytes('\n')
	if err != nil {
		return 0, fmt.Errorf("read chunk header: %w", err)
	}
	line = bytes.TrimSuffix(line, []byte{'\n'})
	line = bytes.TrimSuffix(line, []byte{'\r'})
	sizeStr := line
	if idx := bytes.IndexByte(line, ';'); idx >= 0 {
		sizeStr = line[:idx]
	}
	size, err := strconv.ParseInt(string(bytes.TrimSpace(sizeStr)), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse chunk size %q: %w", sizeStr, err)
	}
	if size < 0 {
		return 0, fmt.Errorf("read chunk data: negative chunk size")
	}
	return size, nil
}

func (r *awsChunkedReader) consumeChunkCRLF() error {
	b, err := r.br.ReadByte()
	if err != nil {
		return fmt.Errorf("read chunk trailer: %w", err)
	}
	if b == '\n' {
		return nil
	}
	if b != '\r' {
		return fmt.Errorf("read chunk trailer: invalid line ending")
	}
	b, err = r.br.ReadByte()
	if err != nil {
		return fmt.Errorf("read chunk trailer: %w", err)
	}
	if b != '\n' {
		return fmt.Errorf("read chunk trailer: invalid line ending")
	}
	return nil
}

func (r *awsChunkedReader) consumeTrailers() error {
	for {
		line, err := r.br.ReadBytes('\n')
		if err != nil {
			return fmt.Errorf("read final chunk trailer: %w", err)
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 {
			return nil
		}
	}
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
