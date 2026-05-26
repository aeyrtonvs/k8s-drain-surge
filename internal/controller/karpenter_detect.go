package controller

import (
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// karpenterBudgetBlockSubstr is the marker Karpenter puts in the message of
// the DisruptionBlocked event it emits against a NodePool when a disruption
// budget (spec.disruption.budgets) forbids consolidation right now (e.g.
// `nodes: "0"` outside a scheduled window). Distinct from the per-Node/
// NodeClaim PDB-rejection event ("Pdb prevents pod evictions").
const karpenterBudgetBlockSubstr = "blocking budget"

// budgetBlocksConsolidation reports whether Karpenter is currently unable to
// consolidate the given NodePool because of a disruption *budget* (not a PDB).
//
// Karpenter emits DisruptionBlocked over the NodePool object with a message
// containing "blocking budget" while a budget forbids disruption, and retries
// every pollingPeriod (10s). We treat the budget as blocking if such an event
// exists for this NodePool with lastTimestamp within ttl. The discriminant is
// structural first — the event must be on involvedObject.kind == "NodePool"
// (PDB-rejection events are emitted on Node/NodeClaim) — and the substring is
// a secondary confirmation, so a future wording tweak upstream does not
// silently flip the gate.
//
// Returns false when no fresh budget-block event is found, meaning the budget
// is (probably) open and a surge can usefully unblock the PDB. now and ttl are
// injected for testability.
func budgetBlocksConsolidation(events []corev1.Event, nodePool string, now time.Time, ttl time.Duration) bool {
	if nodePool == "" {
		return false
	}
	for i := range events {
		e := &events[i]
		if e.Reason != "DisruptionBlocked" {
			continue
		}
		if e.InvolvedObject.Kind != "NodePool" || e.InvolvedObject.Name != nodePool {
			continue
		}
		if !strings.Contains(e.Message, karpenterBudgetBlockSubstr) {
			continue
		}
		ts := eventLastSeen(e)
		if ts.IsZero() {
			// No usable timestamp. Do NOT treat as blocking: a stuck/garbage
			// event with no timestamp would otherwise latch the gate closed
			// forever (no ttl can expire it), permanently denying surge. The
			// failure is asymmetric — blocking-on-uncertain risks permanent
			// denial that needs human intervention, while not-blocking risks
			// at most one churn cycle that self-heals on the next real
			// (timestamped) event. Karpenter's recorder always stamps
			// EventTime, so this path is effectively unreachable for genuine
			// events anyway.
			continue
		}
		if now.Sub(ts) <= ttl {
			return true
		}
	}
	return false
}

// eventLastSeen returns the most recent timestamp on an Event, preferring
// the series/lastTimestamp fields and falling back to eventTime. Core v1
// Events populate LastTimestamp; the newer events.k8s.io shape uses
// EventTime/series, surfaced here through the corev1 alias fields.
func eventLastSeen(e *corev1.Event) time.Time {
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp.Time
	}
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		return e.Series.LastObservedTime.Time
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	return time.Time{}
}

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
