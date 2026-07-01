package admincli

import (
	"github.com/gritive/GrainFS/internal/adminapi"
)

// Error is the typed error returned on non-2xx admin responses.
// Aliased to adminapi.Error so transport-level handling is shared with
// clusteradmin while admincli keeps endpoint-specific Detail views.
type Error = adminapi.Error

// IsCode reports whether err is a *Error with the given code.
func IsCode(err error, code string) bool { return adminapi.IsCode(err, code) }
