package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func (s *Server) writeIcebergMetadataObject(ctx context.Context, location string, metadata json.RawMessage) error {
	bucket, key, ok := parseS3Location(location)
	if !ok {
		return fmt.Errorf("invalid Iceberg metadata location: %s", location)
	}
	var err error
	if s.iamStore != nil && s.iamStore.AuthEnabled() {
		_, err = s.ops.PutObject(ctx, bucket, key, bytes.NewReader(metadata), "application/json")
	} else {
		_, err = s.ops.PutObjectWithACL(ctx, bucket, key, bytes.NewReader(metadata), "application/json", uint8(s3auth.ACLPublicRead))
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func parseS3Location(location string) (bucket, key string, ok bool) {
	const prefix = "s3://"
	if !strings.HasPrefix(location, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(location, prefix)
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", false
	}
	return rest[:slash], rest[slash+1:], true
}
