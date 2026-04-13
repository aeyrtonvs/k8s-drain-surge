package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestDeploymentWorkload_GetReplicas_NilDefaults(t *testing.T) {
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{Replicas: nil},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if got := wl.GetReplicas(); got != 1 {
		t.Fatalf("expected 1 for nil replicas, got %d", got)
	}
}

func TestDeploymentWorkload_GetReplicas_ExplicitValue(t *testing.T) {
	rep := int32(3)
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{Replicas: &rep},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if got := wl.GetReplicas(); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestDeploymentWorkload_SetReplicas(t *testing.T) {
	rep := int32(1)
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{Replicas: &rep},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	wl.SetReplicas(5)
	if got := wl.GetReplicas(); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestDeploymentWorkload_IsStable_ObservedGeneration(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 3},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 3},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if !wl.IsStable() {
		t.Fatal("expected stable when ObservedGeneration matches Generation")
	}
}

func TestDeploymentWorkload_IsStable_Progressing(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 3},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Conditions: []appsv1.DeploymentCondition{
				{
					Type:   appsv1.DeploymentProgressing,
					Reason: reasonNewRSAvailable,
				},
			},
		},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if !wl.IsStable() {
		t.Fatal("expected stable when Progressing condition has NewReplicaSetAvailable reason")
	}
}

func TestDeploymentWorkload_IsStable_MidRollout(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 3},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Conditions: []appsv1.DeploymentCondition{
				{
					Type:   appsv1.DeploymentProgressing,
					Reason: "ReplicaSetUpdated",
				},
			},
		},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if wl.IsStable() {
		t.Fatal("expected NOT stable during mid-rollout")
	}
}

func TestDeploymentWorkload_CanSurge_RollingUpdate(t *testing.T) {
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType},
		},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if !wl.CanSurge() {
		t.Fatal("expected CanSurge=true for RollingUpdate strategy")
	}
}

func TestDeploymentWorkload_CanSurge_Recreate(t *testing.T) {
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
		},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	if wl.CanSurge() {
		t.Fatal("expected CanSurge=false for Recreate strategy")
	}
}

func TestDeploymentWorkload_GetPodSelector(t *testing.T) {
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
		},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	sel := wl.GetPodSelector()
	if !sel.Matches(labels.Set{"app": "test"}) {
		t.Fatal("expected selector to match {app: test}")
	}
	if sel.Matches(labels.Set{"app": "other"}) {
		t.Fatal("expected selector to NOT match {app: other}")
	}
}

func TestDeploymentWorkload_GetPodSelector_Nil(t *testing.T) {
	dep := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{Selector: nil},
	}
	wl := &DeploymentWorkload{Deployment: dep}
	sel := wl.GetPodSelector()
	if sel.Matches(map[string]string{"app": "test"}) {
		t.Fatal("expected nil selector to match nothing")
	}
}

func TestDeploymentWorkload_GetObjectKind(t *testing.T) {
	wl := &DeploymentWorkload{Deployment: &appsv1.Deployment{}}
	if wl.GetObjectKind() != "Deployment" {
		t.Fatalf("expected 'Deployment', got %s", wl.GetObjectKind())
	}
}
