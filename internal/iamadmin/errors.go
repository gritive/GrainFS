package iamadmin

import "errors"

// ErrEmptyName is returned by RunSACreate when Name is empty. Mirrors the
// behavior of the existing cmd-side validation indirectly via cobra Args.
var ErrEmptyName = errors.New("iam: sa name required")
