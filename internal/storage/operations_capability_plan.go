package storage

type aclCapabilityPlan struct {
	atomicPutter AtomicACLPutter
	aclSetter    ACLSetter
	rollback     ObjectVersionDeleter
}

func buildACLCapabilityPlan(backend Backend) aclCapabilityPlan {
	var plan aclCapabilityPlan
	allowAtomicBehindCurrent := true

	for b := backend; b != nil; b = unwrapOperationBackend(b) {
		next := unwrapOperationBackend(b)
		if plan.atomicPutter == nil && allowAtomicBehindCurrent {
			if atomic, ok := b.(AtomicACLPutter); ok {
				plan.atomicPutter = atomic
			}
		}
		if plan.aclSetter == nil {
			if setter, ok := b.(ACLSetter); ok {
				plan.aclSetter = setter
			}
		}
		if plan.rollback == nil {
			if _, blocksWrites := b.(*RecoveryWriteGate); !blocksWrites {
				if deleter, ok := b.(ObjectVersionDeleter); ok {
					plan.rollback = deleter
				}
			}
		}
		if next != nil && plan.atomicPutter == nil {
			allowAtomicBehindCurrent = false
		}
	}
	return plan
}

// aclPlanForCall mirrors planForCall but caches the ACL discovery rules
// (allowAtomicBehindCurrent). It uses its own aclPlanGen counter so that
// rebuilding either cache cannot make the other look fresh against a stale
// pointer — both are validated against the same upstream genSource but
// invalidate independently.
func (o *Operations) aclPlanForCall() aclCapabilityPlan {
	current := o.currentGeneration()
	if cached := o.aclPlan.Load(); cached != nil && o.aclPlanGen.Load() == current {
		return *cached
	}
	return o.rebuildACLPlan(current)
}

func (o *Operations) rebuildACLPlan(current uint64) aclCapabilityPlan {
	for {
		plan := buildACLCapabilityPlan(o.backend)
		endGen := o.currentGeneration()
		if current == endGen {
			o.aclPlan.Store(&plan)
			o.aclPlanGen.Store(current)
			return plan
		}
		current = endGen
	}
}

func rollbackPutObjectWithACL(plan aclCapabilityPlan, bucket, key, versionID string) error {
	if plan.rollback == nil {
		return UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed}
	}
	return plan.rollback.DeleteObjectVersion(bucket, key, versionID)
}
