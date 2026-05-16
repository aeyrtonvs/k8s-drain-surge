package controller

import (
	"testing"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
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
	if sel.Matches(labels.Set{"app": "test"}) {
		t.Fatal("expected nil selector to match nothing")
	}
}

func TestDeploymentWorkload_GetObjectKind(t *testing.T) {
	wl := &DeploymentWorkload{Deployment: &appsv1.Deployment{}}
	if wl.GetObjectKind() != "Deployment" {
		t.Fatalf("expected 'Deployment', got %s", wl.GetObjectKind())
	}
}

func TestRolloutWorkload_IsStableForRestart(t *testing.T) {
	cases := []struct {
		name    string
		phase   rolloutsv1alpha1.RolloutPhase
		message string
		want    bool
	}{
		{"healthy", rolloutsv1alpha1.RolloutPhaseHealthy, "", true},
		{"progressing-restart", rolloutsv1alpha1.RolloutPhaseProgressing, "rollout is restarting", true},
		{"progressing-deploy", rolloutsv1alpha1.RolloutPhaseProgressing, "more replicas need to be updated", false},
		{"progressing-empty-message", rolloutsv1alpha1.RolloutPhaseProgressing, "", false},
		{"degraded", rolloutsv1alpha1.RolloutPhaseDegraded, "rollout is restarting", false},
		{"paused", rolloutsv1alpha1.RolloutPhasePaused, "rollout is restarting", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ro := &rolloutsv1alpha1.Rollout{
				Status: rolloutsv1alpha1.RolloutStatus{
					Phase:   tc.phase,
					Message: tc.message,
				},
			}
			wl := &RolloutWorkload{Rollout: ro}
			if got := wl.IsStableForRestart(); got != tc.want {
				t.Fatalf("phase=%s message=%q: got %v, want %v", tc.phase, tc.message, got, tc.want)
			}
		})
	}
}
