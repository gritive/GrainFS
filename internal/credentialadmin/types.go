// Package credentialadmin contains the protocol credential CLI's business
// logic. cmd/grainfs is a thin cobra wrapper around these operations.
package credentialadmin

import (
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type CreateReq = adminapi.CredentialCreateReq
type Credential = adminapi.CredentialResp
type ListResp = adminapi.CredentialListResp
type RevokeResp = adminapi.CredentialRevokeResp

type BaseOptions struct {
	Endpoint string
	JSONOut  bool
	Timeout  time.Duration
	Stdout   io.Writer
	Stderr   io.Writer
}

type CreateOptions struct {
	BaseOptions
	SAID      string
	Protocol  string
	Resource  string
	Mode      string
	ExpiresAt string
}

type ListOptions struct {
	BaseOptions
	SAID     string
	Protocol string
}

type GetOptions struct {
	BaseOptions
	ID string
}

type RotateOptions struct {
	BaseOptions
	ID string
}

type RevokeOptions struct {
	BaseOptions
	ID string
}
