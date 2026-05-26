package controller

import (
	"testing"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func pdb(minAvail, maxUnav *intstr.IntOrString) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable:   minAvail,
			MaxUnavailable: maxUnav,
		},
	}
}

func intIS(n int) *intstr.IntOrString    { v := intstr.FromInt(n); return &v }
func pctIS(s string) *intstr.IntOrString { v := intstr.FromString(s); return &v }

func TestPDBWouldAllowSurge(t *testing.T) {
	tests := []struct {
		name   string
		pdb    *policyv1.PodDisruptionBudget
		target int32
		want   bool
	}{
		{"minAvailable 1 absolute, target=2 (case A)", pdb(intIS(1), nil), 2, true},
		{"minAvailable 2 absolute, target=2 (case F)", pdb(intIS(2), nil), 2, false},
		{"minAvailable 3 absolute, target=4", pdb(intIS(3), nil), 4, true},
		{"minAvailable 100% percent (case G)", pdb(pctIS("100%"), nil), 5, false},
		{"minAvailable 50% percent, target=4", pdb(pctIS("50%"), nil), 4, true},
		{"minAvailable 0% percent, target=2", pdb(pctIS("0%"), nil), 2, true},
		{"maxUnavailable 1 absolute", pdb(nil, intIS(1)), 2, true},
		{"maxUnavailable 0 absolute", pdb(nil, intIS(0)), 2, false},
		{"maxUnavailable 50% percent, target=2 (case B)", pdb(nil, pctIS("50%")), 2, true},
		{"maxUnavailable 0% percent", pdb(nil, pctIS("0%")), 5, false},
		{"target=0 sanity", pdb(intIS(1), nil), 0, false},
		{"nil pdb", nil, 2, false},
		{"both fields present, both permissive", pdb(intIS(1), intIS(1)), 2, true},
		{"both fields present, min blocks", pdb(intIS(2), intIS(1)), 2, false},
		{"both fields present, max blocks", pdb(intIS(1), intIS(0)), 2, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pdbWouldAllowSurge(tc.pdb, tc.target)
			if got != tc.want {
				t.Fatalf("pdbWouldAllowSurge: want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestIsPDBBlocked(t *testing.T) {
	tests := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want bool
	}{
		{"nil", nil, false},
		{"disruptionsAllowed=0 with expectedPods=1", &policyv1.PodDisruptionBudget{
			Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 1},
		}, true},
		{"disruptionsAllowed=1", &policyv1.PodDisruptionBudget{
			Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1, ExpectedPods: 1},
		}, false},
		{"no expected pods", &policyv1.PodDisruptionBudget{
			Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 0},
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPDBBlocked(tc.pdb); got != tc.want {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestGracePeriodTracker(t *testing.T) {
	tr := newGracePeriodTracker()
	grace := 60 * time.Second
	uid := types.UID("pdb-uid-1")
	t0 := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)

	if tr.Observe(uid, true, t0, grace) {
		t.Fatalf("first observation should not elapse")
	}
	if tr.Observe(uid, true, t0.Add(30*time.Second), grace) {
		t.Fatalf("30s in should not elapse")
	}
	if !tr.Observe(uid, true, t0.Add(60*time.Second), grace) {
		t.Fatalf("60s in should elapse")
	}

	tr.Observe(uid, false, t0.Add(2*time.Minute), grace)
	if tr.Observe(uid, true, t0.Add(2*time.Minute+1*time.Second), grace) {
		t.Fatalf("after unblock+reblock, first observation should not elapse")
	}

	tr.Forget(uid)
	if tr.Observe(uid, true, t0.Add(10*time.Minute), grace) {
		t.Fatalf("after Forget, next observation should not elapse")
	}
}
