package pullthrough

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gritive/GrainFS/internal/storage"
)

// S3Upstream fetches objects from an S3-compatible upstream endpoint.
type S3Upstream struct {
	client *s3.Client
}

// NewS3Upstream creates an upstream that pulls from the given S3-compatible endpoint.
// accessKey and secretKey are the S3 credentials for the upstream endpoint.
func NewS3Upstream(endpoint, accessKey, secretKey string) (*S3Upstream, error) {
	var creds aws.CredentialsProvider
	if accessKey != "" && secretKey != "" {
		creds = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	} else {
		creds = aws.AnonymousCredentials{}
	}
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  creds,
		UsePathStyle: true,
		HTTPClient:   &http.Client{Timeout: 30 * time.Second},
	})
	return &S3Upstream{client: client}, nil
}

// GetObject fetches a single object from the upstream S3 endpoint.
func (u *S3Upstream) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	out, err := u.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, nil, storage.ErrObjectNotFound
		}
		return nil, nil, err
	}

	ct := ""
	if out.ContentType != nil {
		ct = *out.ContentType
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	return out.Body, &storage.Object{Key: key, Size: size, ContentType: ct}, nil
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") ||
		strings.Contains(msg, "NotFound") ||
		strings.Contains(msg, "no such key")
}
