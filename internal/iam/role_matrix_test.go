package iam

import (
	"testing"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestRoleAllows_Matrix(t *testing.T) {
	cases := []struct {
		role   Role
		action s3auth.S3Action
		want   bool
	}{
		// Read role: GET, HEAD, LIST allowed; everything else denied.
		{RoleRead, s3auth.GetObject, true},
		{RoleRead, s3auth.HeadObject, true},
		{RoleRead, s3auth.ListBucket, true},
		{RoleRead, s3auth.ListMultipartUploads, true},
		{RoleRead, s3auth.PutObject, false},
		{RoleRead, s3auth.DeleteObject, false},
		{RoleRead, s3auth.CopyObject, false},
		{RoleRead, s3auth.CreateBucket, false},
		{RoleRead, s3auth.DeleteBucket, false},

		// Write role: Read + PUT, DELETE on objects within an existing bucket
		// + CopyObject. Not bucket lifecycle.
		{RoleWrite, s3auth.GetObject, true},
		{RoleWrite, s3auth.HeadObject, true},
		{RoleWrite, s3auth.ListBucket, true},
		{RoleWrite, s3auth.ListMultipartUploads, true},
		{RoleWrite, s3auth.PutObject, true},
		{RoleWrite, s3auth.DeleteObject, true},
		{RoleWrite, s3auth.CopyObject, true},
		{RoleWrite, s3auth.CreateBucket, false},
		{RoleWrite, s3auth.DeleteBucket, false},

		// Admin role: full set including bucket lifecycle.
		{RoleAdmin, s3auth.GetObject, true},
		{RoleAdmin, s3auth.HeadObject, true},
		{RoleAdmin, s3auth.ListBucket, true},
		{RoleAdmin, s3auth.ListMultipartUploads, true},
		{RoleAdmin, s3auth.PutObject, true},
		{RoleAdmin, s3auth.DeleteObject, true},
		{RoleAdmin, s3auth.CopyObject, true},
		{RoleAdmin, s3auth.CreateBucket, true},
		{RoleAdmin, s3auth.DeleteBucket, true},

		// None role: deny everything.
		{RoleNone, s3auth.GetObject, false},
		{RoleNone, s3auth.PutObject, false},
		{RoleNone, s3auth.CreateBucket, false},

		// UnknownAction: deny under every role.
		{RoleRead, s3auth.UnknownAction, false},
		{RoleWrite, s3auth.UnknownAction, false},
		{RoleAdmin, s3auth.UnknownAction, false},
	}
	for _, c := range cases {
		got := RoleAllows(c.role, c.action)
		if got != c.want {
			t.Errorf("RoleAllows(%v, %d) = %v, want %v", c.role, c.action, got, c.want)
		}
	}
}
