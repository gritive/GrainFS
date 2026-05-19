package iam

import "github.com/google/uuid"

// NewUUIDv7 returns a fresh UUIDv7 string for IAM entity IDs (SA, group, policy).
func NewUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// uuid.NewV7 only errors on rand reader failure; treat as fatal.
		panic("iam: uuid.NewV7: " + err.Error())
	}
	return id.String()
}
