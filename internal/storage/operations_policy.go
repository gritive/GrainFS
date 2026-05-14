package storage

func (o *Operations) SetBucketPolicy(bucket string, policyJSON []byte) error {
	plan := o.planForCall()
	if plan.policyBackend != nil {
		if err := plan.policyBackend.SetBucketPolicy(bucket, policyJSON); err != nil {
			return err
		}
	} else if o.policyStore == nil {
		return UnsupportedOperationError{Op: "SetBucketPolicy", Reason: UnsupportedReasonNoAdapter}
	}
	if o.policyStore != nil {
		return o.policyStore.Set(bucket, policyJSON)
	}
	return nil
}

func (o *Operations) GetBucketPolicy(bucket string) ([]byte, error) {
	if o.policyStore != nil {
		if raw := o.policyStore.GetRaw(bucket); raw != nil {
			return raw, nil
		}
	}
	plan := o.planForCall()
	if plan.policyBackend == nil {
		return nil, UnsupportedOperationError{Op: "GetBucketPolicy", Reason: UnsupportedReasonNoAdapter}
	}
	data, err := plan.policyBackend.GetBucketPolicy(bucket)
	if err != nil {
		return nil, err
	}
	if o.policyStore != nil {
		// Ignore compilation errors: an existing policy with a non-conforming Effect
		// (e.g. stored before Effect validation was added) must still be readable via
		// the admin API. The raw bytes are returned; only the in-memory compiled cache
		// is skipped, so S3 auth for that bucket defaults to deny until the policy is
		// rewritten with a valid Effect.
		_ = o.policyStore.Set(bucket, data)
	}
	return data, nil
}

func (o *Operations) DeleteBucketPolicy(bucket string) error {
	plan := o.planForCall()
	if plan.policyBackend != nil {
		if err := plan.policyBackend.DeleteBucketPolicy(bucket); err != nil {
			return err
		}
	} else if o.policyStore == nil {
		return UnsupportedOperationError{Op: "DeleteBucketPolicy", Reason: UnsupportedReasonNoAdapter}
	}
	if o.policyStore != nil {
		o.policyStore.Delete(bucket)
	}
	return nil
}
