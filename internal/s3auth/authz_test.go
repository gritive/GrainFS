package s3auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestACLGrantConstants(t *testing.T) {
	assert.Equal(t, ACLGrant(0), ACLPrivate)
	assert.Equal(t, ACLGrant(1), ACLPublicRead)
	assert.Equal(t, ACLGrant(2), ACLPublicReadWrite)

	// bitmask 독립성: Read와 ReadWrite는 별도 비트
	assert.NotEqual(t, ACLPublicRead, ACLPublicReadWrite)
	assert.Zero(t, ACLPrivate&ACLPublicRead)
}

func TestIsReadAction(t *testing.T) {
	readActions := []S3Action{GetObject, HeadObject, ListBucket}
	for _, a := range readActions {
		assert.True(t, isReadAction(a), "expected %v to be a read action", a)
	}

	writeActions := []S3Action{PutObject, DeleteObject, CreateBucket, DeleteBucket}
	for _, a := range writeActions {
		assert.False(t, isReadAction(a), "expected %v to NOT be a read action", a)
	}
}

func TestIsAuthorizedByACL(t *testing.T) {
	tests := []struct {
		name      string
		acl       ACLGrant
		accessKey string
		action    S3Action
		want      bool
	}{
		// ACLPrivate: 인증된 사용자만 허용
		{"private + authenticated + read", ACLPrivate, "user1", GetObject, true},
		{"private + authenticated + write", ACLPrivate, "user1", PutObject, true},
		{"private + anonymous + read", ACLPrivate, "", GetObject, false},
		{"private + anonymous + write", ACLPrivate, "", PutObject, false},

		// ACLPublicRead: read는 익명 포함 허용, write는 인증 필요
		{"public-read + anonymous + read", ACLPublicRead, "", GetObject, true},
		{"public-read + anonymous + list", ACLPublicRead, "", ListBucket, true},
		{"public-read + anonymous + head", ACLPublicRead, "", HeadObject, true},
		{"public-read + anonymous + write", ACLPublicRead, "", PutObject, false},
		{"public-read + anonymous + delete", ACLPublicRead, "", DeleteObject, false},
		{"public-read + authenticated + write", ACLPublicRead, "user1", PutObject, true},

		// ACLPublicReadWrite: 모든 action 익명 포함 허용
		{"public-rw + anonymous + read", ACLPublicReadWrite, "", GetObject, true},
		{"public-rw + anonymous + write", ACLPublicReadWrite, "", PutObject, true},
		{"public-rw + anonymous + delete", ACLPublicReadWrite, "", DeleteObject, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsAuthorizedByACL(tt.acl, tt.accessKey, tt.action)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseACLHeader(t *testing.T) {
	tests := []struct {
		input string
		want  ACLGrant
	}{
		{"private", ACLPrivate},
		{"public-read", ACLPublicRead},
		{"public-read-write", ACLPublicReadWrite},
		{"", ACLPrivate},              // 빈 값 → private
		{"unknown-value", ACLPrivate}, // 알 수 없는 값 → private fallback
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, ParseACLHeader(tt.input))
		})
	}
}
