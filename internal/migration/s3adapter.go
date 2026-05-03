package migration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/gritive/GrainFS/internal/storage"
)

func newS3Client(endpoint, accessKey, secretKey string) *s3.Client {
	var creds aws.CredentialsProvider
	if accessKey != "" && secretKey != "" {
		creds = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	} else {
		creds = aws.AnonymousCredentials{}
	}
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  creds,
		UsePathStyle: true,
	})
}

// S3Source implements Source for an S3-compatible endpoint.
type S3Source struct {
	client *s3.Client
}

// NewS3Source creates a Source backed by an S3-compatible endpoint.
func NewS3Source(endpoint, accessKey, secretKey string) *S3Source {
	return &S3Source{client: newS3Client(endpoint, accessKey, secretKey)}
}

func (s *S3Source) ListBuckets() ([]string, error) {
	out, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	names := make([]string, len(out.Buckets))
	for i, b := range out.Buckets {
		names[i] = aws.ToString(b.Name)
	}
	return names, nil
}

func (s *S3Source) ListObjects(bucket string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

func (s *S3Source) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, nil, storage.ErrObjectNotFound
		}
		return nil, nil, err
	}
	ct := aws.ToString(out.ContentType)
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, &storage.Object{Key: key, Size: size, ContentType: ct}, nil
}

// S3Destination implements Destination for an S3-compatible endpoint.
type S3Destination struct {
	client *s3.Client
}

// NewS3Destination creates a Destination backed by an S3-compatible endpoint.
func NewS3Destination(endpoint, accessKey, secretKey string) *S3Destination {
	return &S3Destination{client: newS3Client(endpoint, accessKey, secretKey)}
}

func (d *S3Destination) CreateBucket(ctx context.Context, bucket string) error {
	_, err := d.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		// BucketAlreadyOwnedByYou or BucketAlreadyExists → treat as already exists
		if isAlreadyExists(err) {
			return storage.ErrBucketAlreadyExists
		}
		return err
	}
	return nil
}

func (d *S3Destination) PutObject(ctx context.Context, bucket, key string, body io.Reader, ct string) (*storage.Object, error) {
	// S3 SDK requires a seekable body for payload hash computation.
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	if ct != "" {
		input.ContentType = aws.String(ct)
	}
	_, err = d.client.PutObject(ctx, input)
	if err != nil {
		return nil, err
	}
	return &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (d *S3Destination) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, nil, storage.ErrObjectNotFound
		}
		return nil, nil, err
	}
	ct := aws.ToString(out.ContentType)
	return out.Body, &storage.Object{Key: key, ContentType: ct}, nil
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

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var alreadyOwned *s3types.BucketAlreadyOwnedByYou
	var alreadyExists *s3types.BucketAlreadyExists
	if errors.As(err, &alreadyOwned) || errors.As(err, &alreadyExists) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "BucketAlreadyOwnedByYou") ||
		strings.Contains(msg, "BucketAlreadyExists")
}
