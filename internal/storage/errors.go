package storage

import "errors"

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrObjectNotFound      = errors.New("object not found")
	ErrUploadNotFound      = errors.New("upload not found")
	ErrInvalidPart         = errors.New("invalid part")
	ErrMethodNotAllowed    = errors.New("method not allowed on delete marker")
)
