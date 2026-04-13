package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func scalerScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = policyv1.AddToScheme(s)
	_ = autoscalingv2.AddToScheme(s)
	return s
}

func scalerClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scalerScheme()).
		WithObjects(objs...).
		Build()
}

func TestResolveWorkloadFromPod_Deployment(t *testing.T) {
	ctx := context.Background()
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-dep", Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "my-dep"}},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-dep-rs1",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "Deployment", Name: "my-dep"},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-dep-pod1",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "my-dep-rs1"},
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	c := scalerClient(dep, rs, pod)

	wl, err := ResolveWorkloadFromPod(ctx, c, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wl == nil {
		t.Fatal("expected workload, got nil")
	}
	if wl.GetObjectKind() != "Deployment" {
		t.Fatalf("expected Deployment, got %s", wl.GetObjectKind())
	}
	if wl.GetObjectMeta().Name != "my-dep" {
		t.Fatalf("expected name=my-dep, got %s", wl.GetObjectMeta().Name)
	}
}

func TestResolveWorkloadFromPod_NoOwner(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"},
	}
	c := scalerClient(pod)

	wl, err := ResolveWorkloadFromPod(ctx, c, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wl != nil {
		t.Fatal("expected nil workload for pod without RS owner")
	}
}

func TestFindMatchingPDB_Match(t *testing.T) {
	ctx := context.Background()
	minAvail := intstr.FromInt(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb1", Namespace: "default"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvail,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		},
	}
	c := scalerClient(pdb)

	sel := labels.SelectorFromSet(map[string]string{"app": "test"})
	found, err := FindMatchingPDB(ctx, c, "default", sel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected PDB to be found")
	}
}

func TestFindMatchingPDB_NoMatch(t *testing.T) {
	ctx := context.Background()
	minAvail := intstr.FromInt(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb1", Namespace: "default"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvail,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}},
		},
	}
	c := scalerClient(pdb)

	sel := labels.SelectorFromSet(map[string]string{"app": "test"})
	found, err := FindMatchingPDB(ctx, c, "default", sel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected no PDB match")
	}
}

func TestFindMatchingPDB_NoPDBs(t *testing.T) {
	ctx := context.Background()
	c := scalerClient()

	sel := labels.SelectorFromSet(map[string]string{"app": "test"})
	found, err := FindMatchingPDB(ctx, c, "default", sel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected no PDB match with empty list")
	}
}

func TestCheckHPACompatibility_NoHPA(t *testing.T) {
	ctx := context.Background()
	c := scalerClient()

	compatible, exists, err := CheckHPACompatibility(ctx, c, "default", "my-dep", "Deployment")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("expected no HPA")
	}
	if !compatible {
		t.Fatal("expected compatible when no HPA exists")
	}
}

func TestCheckHPACompatibility_MaxReplicas1(t *testing.T) {
	ctx := context.Background()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "my-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				Kind: "Deployment",
				Name: "my-dep",
			},
			MaxReplicas: 1,
		},
	}
	c := scalerClient(hpa)

	compatible, exists, err := CheckHPACompatibility(ctx, c, "default", "my-dep", "Deployment")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected HPA to exist")
	}
	if compatible {
		t.Fatal("expected not compatible with maxReplicas=1")
	}
}

func TestCheckHPACompatibility_MaxReplicasGt1(t *testing.T) {
	ctx := context.Background()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "my-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				Kind: "Deployment",
				Name: "my-dep",
			},
			MaxReplicas: 5,
		},
	}
	c := scalerClient(hpa)

	compatible, exists, err := CheckHPACompatibility(ctx, c, "default", "my-dep", "Deployment")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected HPA to exist")
	}
	if !compatible {
		t.Fatal("expected compatible with maxReplicas=5")
	}
}

func TestFindReadyPodOnOtherNode(t *testing.T) {
	ctx := context.Background()
	readyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-ready",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-2"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	c := scalerClient(readyPod)
	sel := labels.SelectorFromSet(map[string]string{"app": "test"})

	found, err := FindReadyPodOnOtherNode(ctx, c, "default", sel, "node-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected to find ready pod on other node")
	}

	// Same node — should not find.
	found, err = FindReadyPodOnOtherNode(ctx, c, "default", sel, "node-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected NOT to find ready pod on same node")
	}
}

func TestFindPodOnNode(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-on-node",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c := scalerClient(pod)
	sel := labels.SelectorFromSet(map[string]string{"app": "test"})

	found, err := FindPodOnNode(ctx, c, "default", sel, "node-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected to find pod on node-1")
	}

	found, err = FindPodOnNode(ctx, c, "default", sel, "node-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected NOT to find pod on node-2")
	}
}

func TestRequirementsMatch(t *testing.T) {
	sel1 := labels.SelectorFromSet(map[string]string{"app": "test"})
	sel2 := labels.SelectorFromSet(map[string]string{"app": "test"})
	sel3 := labels.SelectorFromSet(map[string]string{"app": "other"})

	r1, _ := sel1.Requirements()
	r2, _ := sel2.Requirements()
	r3, _ := sel3.Requirements()

	if !requirementsMatch(r1, r2) {
		t.Fatal("expected match for identical selectors")
	}
	if requirementsMatch(r1, r3) {
		t.Fatal("expected no match for different selectors")
	}

}
