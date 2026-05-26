package controller

import (
	"testing"

	policyv1 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func mkPDB(disruptionsAllowed, expectedPods int32) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		Status: policyv1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: disruptionsAllowed,
			ExpectedPods:       expectedPods,
		},
	}
}

func TestKarpenterPDBPredicate(t *testing.T) {
	p := karpenterPDBPredicate()

	// Create
	if !p.Create(event.CreateEvent{Object: mkPDB(0, 1)}) {
		t.Fatalf("Create disruptionsAllowed=0 expected=1 should accept")
	}
	if p.Create(event.CreateEvent{Object: mkPDB(1, 1)}) {
		t.Fatalf("Create disruptionsAllowed=1 should reject")
	}
	if p.Create(event.CreateEvent{Object: mkPDB(0, 0)}) {
		t.Fatalf("Create with no expected pods should reject")
	}

	// Update — transitions
	if !p.Update(event.UpdateEvent{
		ObjectOld: mkPDB(1, 1),
		ObjectNew: mkPDB(0, 1),
	}) {
		t.Fatalf("Update 1→0 should accept")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: mkPDB(0, 1),
		ObjectNew: mkPDB(1, 1),
	}) {
		t.Fatalf("Update 0→1 should reject")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: mkPDB(0, 1),
		ObjectNew: mkPDB(0, 1),
	}) {
		t.Fatalf("Update 0→0 should reject (already blocked, no transition)")
	}
	// currentHealthy / expectedPods cosmetic changes (still blocked → blocked).
	oldP := mkPDB(0, 1)
	oldP.Status.CurrentHealthy = 1
	newP := mkPDB(0, 2)
	newP.Status.CurrentHealthy = 2
	if p.Update(event.UpdateEvent{ObjectOld: oldP, ObjectNew: newP}) {
		t.Fatalf("Update with unrelated status changes should reject")
	}

	// Delete + Generic always reject.
	if p.Delete(event.DeleteEvent{Object: mkPDB(0, 1)}) {
		t.Fatalf("Delete should reject")
	}
	if p.Generic(event.GenericEvent{Object: mkPDB(0, 1)}) {
		t.Fatalf("Generic should reject")
	}
}
