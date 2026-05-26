package controller

import (
	"sync"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// pdbWouldAllowSurge reports whether scaling the workload from current to
// target replicas would make the PDB allow at least one disruption.
//
// PDBs express their constraint as one of two fields, possibly as a percentage:
//   - spec.minAvailable: at least N (or pct of total) pods must remain available.
//   - spec.maxUnavailable: at most N (or pct of total) pods may be unavailable.
//
// A surge from current=1 to target=2 succeeds iff at target=2 the PDB still
// permits losing 1 pod. The function evaluates whichever field is set and
// returns true only when the result is permissive.
func pdbWouldAllowSurge(pdb *policyv1.PodDisruptionBudget, target int32) bool {
	if pdb == nil || target <= 0 {
		return false
	}

	minAvail := pdb.Spec.MinAvailable
	maxUnav := pdb.Spec.MaxUnavailable

	minOK := true
	if minAvail != nil {
		n, err := intstr.GetScaledValueFromIntOrPercent(minAvail, int(target), true)
		if err != nil {
			return false
		}
		minOK = int32(n) < target
	}

	maxOK := true
	if maxUnav != nil {
		n, err := intstr.GetScaledValueFromIntOrPercent(maxUnav, int(target), false)
		if err != nil {
			return false
		}
		maxOK = n >= 1
	}

	return minOK && maxOK
}

// gracePeriodTracker records, per PDB UID, when we first observed
// `disruptionsAllowed == 0`. The tracker is in-memory and not persisted:
// on controller restart the grace period rearranges itself from the next
// observation — worst case a one-time grace-period of extra delay.
//
// All methods are safe for concurrent use; the reconciler may be invoked
// from multiple worker goroutines and the backup ticker is a separate one.
type gracePeriodTracker struct {
	mu     sync.Mutex
	firstSeen map[types.UID]time.Time
}

func newGracePeriodTracker() *gracePeriodTracker {
	return &gracePeriodTracker{firstSeen: make(map[types.UID]time.Time)}
}

// Observe records that we have seen `disruptionsAllowed == 0` for this PDB
// and returns whether the grace period has fully elapsed. now is injected
// for testability. Calling Observe with `blocked=false` clears the tracker
// entry so a subsequent block restarts the grace timer.
func (t *gracePeriodTracker) Observe(uid types.UID, blocked bool, now time.Time, grace time.Duration) (elapsed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !blocked {
		delete(t.firstSeen, uid)
		return false
	}
	first, ok := t.firstSeen[uid]
	if !ok {
		t.firstSeen[uid] = now
		return false
	}
	return now.Sub(first) >= grace
}

// Forget removes a PDB from the tracker (e.g. after deletion).
func (t *gracePeriodTracker) Forget(uid types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.firstSeen, uid)
}

// isPDBBlocked reports whether the PDB is in the karpenter-stuck state:
// status reports `disruptionsAllowed == 0` and at least one pod is expected.
// `expectedPods == 0` would mean the PDB has no targets and is irrelevant.
func isPDBBlocked(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb == nil {
		return false
	}
	if pdb.Status.ExpectedPods <= 0 {
		return false
	}
	return pdb.Status.DisruptionsAllowed == 0
}
