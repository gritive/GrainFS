package storage

// SetObjectTags dispatches a versionID-aware tag mutation through the
// backend, mirroring SetObjectACL. Returns UnsupportedOperationError if
// the backend does not implement ObjectTagsSetter.
func (o *Operations) SetObjectTags(bucket, key, versionID string, tags []Tag) error {
	plan := o.planForCall()
	if plan.tagsSetter == nil {
		return UnsupportedOperationError{Op: "SetObjectTags", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.tagsSetter.SetObjectTags(bucket, key, versionID, tags)
}

// GetObjectTags returns the tag set for the object. Returns an empty slice
// (never nil) when no tags are present.
func (o *Operations) GetObjectTags(bucket, key, versionID string) ([]Tag, error) {
	plan := o.planForCall()
	if plan.tagsGetter == nil {
		return nil, UnsupportedOperationError{Op: "GetObjectTags", Reason: UnsupportedReasonNoAdapter}
	}
	tags, err := plan.tagsGetter.GetObjectTags(bucket, key, versionID)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		return []Tag{}, nil
	}
	return tags, nil
}

// DeleteObjectTags clears all tags on the object. It is sugar for
// SetObjectTags(..., nil).
func (o *Operations) DeleteObjectTags(bucket, key, versionID string) error {
	return o.SetObjectTags(bucket, key, versionID, nil)
}
