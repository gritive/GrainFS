package s3auth

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// DecodeAWSChunkedBody decodes an aws-chunked encoded body.
// Format: hex_size;chunk-signature=...\r\ndata\r\n...0;chunk-signature=...\r\n\r\n
func DecodeAWSChunkedBody(data []byte) ([]byte, error) {
	r := bytes.NewReader(data)
	var result bytes.Buffer

	for {
		// Read chunk header line: "hex_size;chunk-signature=...\r\n"
		line, err := readLine(r)
		if err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}

		// Extract hex size (before the semicolon)
		sizeStr := line
		if idx := bytes.IndexByte(line, ';'); idx >= 0 {
			sizeStr = line[:idx]
		}

		size, err := strconv.ParseInt(string(bytes.TrimSpace(sizeStr)), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("parse chunk size %q: %w", sizeStr, err)
		}

		if size == 0 {
			break // last chunk
		}

		// Read chunk data
		chunk := make([]byte, size)
		if _, err := io.ReadFull(r, chunk); err != nil {
			return nil, fmt.Errorf("read chunk data: %w", err)
		}
		result.Write(chunk)

		// Read trailing \r\n
		if _, err := readLine(r); err != nil && err != io.EOF {
			return nil, fmt.Errorf("read chunk trailer: %w", err)
		}
	}

	return result.Bytes(), nil
}

// readLine reads until \r\n or \n.
func readLine(r *bytes.Reader) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return line, err
		}
		if b == '\n' {
			// Strip trailing \r
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			return line, nil
		}
		line = append(line, b)
	}
}
