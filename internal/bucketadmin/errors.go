package bucketadmin

import "errors"

// ErrAttachConflict is returned when --attach-sa and --attach-policy aren't
// both set or both unset. Validation lives in cmd's RunE per existing CLI
// behavior; this constant is here for tests that exercise the boundary.
var ErrAttachConflict = errors.New("bucket: --attach-sa and --attach-policy must be set together")
