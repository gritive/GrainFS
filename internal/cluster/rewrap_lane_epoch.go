package cluster

// CurrentRewrapLaneSetEpoch is the lane-set version that a completing node
// stamps on its DEKRewrapProgress report. Consumers that require a minimum
// epoch (e.g. a future S7 prune) can gate on this value via IsGenFullyRewrapped.
//
// Epoch semantics:
//
//	epoch 0: EC shards of all object versions (S6c-allversions, #692) and
//	         packed-blob entries (S6b, #687).
//	epoch 1: epoch 0 + FSM-value check lane (S7-1a-2). A node at epoch 1 has
//	         verified that all its policy:/obj: FSM-values are at keeper-current
//	         gen after the drain marker was applied. A prune predicate gating on
//	         epoch>=1 knows FSM-values are drained on every reporting node.
//
// Bump this constant when a new rewrap lane is registered in wireRewrapLanes.
// Every existing completion report carries the epoch value at the time of
// the sweep; a bumped epoch causes old reports to fail a higher requiredEpoch
// gate until the node re-sweeps and re-reports with the new epoch.
const CurrentRewrapLaneSetEpoch uint32 = 1
