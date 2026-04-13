package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/aeyrton/k8s-drain-surge/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		DrainTaints: []config.DrainTaint{
			{Key: "karpenter.sh/disrupted", Effect: "NoSchedule"},
		},
		EnabledAnnotation: AnnotationEnabled,
		RequeueInterval:   5 * time.Second,
		ReadinessTimeout:  10 * time.Minute,
	}
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = policyv1.AddToScheme(s)
	return s
}

func newReconciler(objs ...client.Object) (*NodeReconciler, client.Client) {
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&appsv1.Deployment{}).
		Build()

	return &NodeReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		Config:   testConfig(),
	}, c
}

func int32Ptr(i int32) *int32 { return &i }

func newTaintedNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: "karpenter.sh/disrupted", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}
}

func newDeployment(name, namespace string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				AnnotationEnabled: "true",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
		},
	}
}

func newReplicaSet(name, namespace, deploymentName string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deploymentName,
				},
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
		},
	}
}

func newPodOnNode(name, namespace, nodeName, rsName string, ready bool) *corev1.Pod {
	conditions := []corev1.PodCondition{}
	if ready {
		conditions = append(conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": "test-app"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       rsName,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: conditions,
		},
	}
}

func newPDB(name, namespace string, matchLabels map[string]string) *policyv1.PodDisruptionBudget {
	minAvailable := intstr.FromInt(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: matchLabels},
		},
	}
}

// TestHappyPath_Deployment_FullCycle tests the complete drain cycle for a Deployment:
// taint detected -> scale up -> new pod ready -> old pod evicted -> scale down -> cleanup.
func TestHappyPath_Deployment_FullCycle(t *testing.T) {
	ctx := context.Background()
	node := newTaintedNode("node-1")
	dep := newDeployment("test-app", "default")
	rs := newReplicaSet("test-app-rs1", "default", "test-app")
	pod := newPodOnNode("test-app-pod1", "default", "node-1", "test-app-rs1", true)
	pdb := newPDB("test-app-pdb", "default", map[string]string{"app": "test-app"})

	r, c := newReconciler(node, dep, rs, pod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	// --- Step 1: First reconcile → should scale up (None -> ScaledUp) ---
	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile step 1: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected requeue after scale-up")
	}

	var updatedDep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if *updatedDep.Spec.Replicas != 2 {
		t.Fatalf("expected replicas=2 after scale-up, got %d", *updatedDep.Spec.Replicas)
	}
	if updatedDep.Annotations[AnnotationDrainState] != string(DrainStateScaledUp) {
		t.Fatalf("expected state=scaled-up, got %s", updatedDep.Annotations[AnnotationDrainState])
	}
	if updatedDep.Annotations[AnnotationOriginalReplicas] != "1" {
		t.Fatalf("expected original-replicas=1, got %s", updatedDep.Annotations[AnnotationOriginalReplicas])
	}
	if updatedDep.Annotations[AnnotationDrainNode] != "node-1" {
		t.Fatalf("expected drain-node=node-1, got %s", updatedDep.Annotations[AnnotationDrainNode])
	}
	if updatedDep.Annotations[AnnotationDrainStart] == "" {
		t.Fatal("expected drain-start to be set")
	}

	// --- Step 2: Reconcile again with no new pod → should stay in scaled-up, waiting ---
	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile step 2: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected requeue while waiting for ready pod")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if updatedDep.Annotations[AnnotationDrainState] != string(DrainStateScaledUp) {
		t.Fatalf("expected state=scaled-up (still waiting), got %s", updatedDep.Annotations[AnnotationDrainState])
	}

	// --- Step 3: Create a new ready pod on node-2 → should transition to Ready ---
	newPod := newPodOnNode("test-app-pod2", "default", "node-2", "test-app-rs1", true)
	if err := c.Create(ctx, newPod); err != nil {
		t.Fatalf("create new pod: %v", err)
	}

	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile step 3: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if updatedDep.Annotations[AnnotationDrainState] != string(DrainStateReady) {
		t.Fatalf("expected state=ready, got %s", updatedDep.Annotations[AnnotationDrainState])
	}

	// --- Step 4: Delete old pod (simulating eviction) → should transition to Draining ---
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatalf("delete old pod: %v", err)
	}

	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile step 4: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if updatedDep.Annotations[AnnotationDrainState] != string(DrainStateDraining) {
		t.Fatalf("expected state=draining, got %s", updatedDep.Annotations[AnnotationDrainState])
	}
	if *updatedDep.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 after scale-down, got %d", *updatedDep.Spec.Replicas)
	}

	// --- Step 5: Reconcile → should transition Draining -> Done ---
	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile step 5: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if updatedDep.Annotations[AnnotationDrainState] != string(DrainStateDone) {
		t.Fatalf("expected state=done, got %s", updatedDep.Annotations[AnnotationDrainState])
	}

	// --- Step 6: Reconcile → should clean up annotations ---
	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile step 6: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if _, exists := updatedDep.Annotations[AnnotationDrainState]; exists {
		t.Fatalf("expected drain-state annotation to be removed, got %s", updatedDep.Annotations[AnnotationDrainState])
	}
	if _, exists := updatedDep.Annotations[AnnotationOriginalReplicas]; exists {
		t.Fatal("expected original-replicas annotation to be removed")
	}
	if _, exists := updatedDep.Annotations[AnnotationDrainNode]; exists {
		t.Fatal("expected drain-node annotation to be removed")
	}
	if _, exists := updatedDep.Annotations[AnnotationDrainStart]; exists {
		t.Fatal("expected drain-start annotation to be removed")
	}

	// The enabled annotation should remain.
	if updatedDep.Annotations[AnnotationEnabled] != "true" {
		t.Fatal("expected enabled annotation to remain")
	}

	// Final replicas should be 1.
	if *updatedDep.Spec.Replicas != 1 {
		t.Fatalf("expected final replicas=1, got %d", *updatedDep.Spec.Replicas)
	}
}

// TestHappyPath_Deployment_NoOptIn verifies that workloads without the opt-in
// annotation are not processed.
func TestHappyPath_Deployment_NoOptIn(t *testing.T) {
	ctx := context.Background()
	node := newTaintedNode("node-1")
	dep := newDeployment("test-app", "default")
	delete(dep.Annotations, AnnotationEnabled) // remove opt-in
	rs := newReplicaSet("test-app-rs1", "default", "test-app")
	pod := newPodOnNode("test-app-pod1", "default", "node-1", "test-app-rs1", true)
	pdb := newPDB("test-app-pdb", "default", map[string]string{"app": "test-app"})

	r, c := newReconciler(node, dep, rs, pod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var updatedDep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if *updatedDep.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 (unchanged), got %d", *updatedDep.Spec.Replicas)
	}
	if _, exists := updatedDep.Annotations[AnnotationDrainState]; exists {
		t.Fatal("expected no drain-state annotation on non-opted-in workload")
	}
}

// TestHappyPath_Deployment_NoPDB verifies that workloads without a matching PDB
// are skipped with a warning.
func TestHappyPath_Deployment_NoPDB(t *testing.T) {
	ctx := context.Background()
	node := newTaintedNode("node-1")
	dep := newDeployment("test-app", "default")
	rs := newReplicaSet("test-app-rs1", "default", "test-app")
	pod := newPodOnNode("test-app-pod1", "default", "node-1", "test-app-rs1", true)
	// No PDB created

	r, c := newReconciler(node, dep, rs, pod)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var updatedDep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if *updatedDep.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 (unchanged), got %d", *updatedDep.Spec.Replicas)
	}
}

// TestHappyPath_Deployment_NoTaint verifies that nodes without a drain taint
// cause workloads in mid-drain to be aborted.
func TestHappyPath_Deployment_TaintRemoved(t *testing.T) {
	ctx := context.Background()
	// Node without taint.
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	dep := newDeployment("test-app", "default")
	dep.Spec.Replicas = int32Ptr(2)
	dep.Annotations[AnnotationDrainState] = string(DrainStateScaledUp)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationDrainNode] = "node-1"
	dep.Annotations[AnnotationDrainStart] = time.Now().UTC().Format(time.RFC3339)

	r, c := newReconciler(node, dep)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var updatedDep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if *updatedDep.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 after abort, got %d", *updatedDep.Spec.Replicas)
	}
	if _, exists := updatedDep.Annotations[AnnotationDrainState]; exists {
		t.Fatal("expected drain annotations to be cleared after abort")
	}
}

// TestHappyPath_Deployment_NodeDeleted verifies that when a node is deleted,
// workloads referencing it are aborted.
func TestHappyPath_Deployment_NodeDeleted(t *testing.T) {
	ctx := context.Background()
	// No node exists, but the deployment references it.
	dep := newDeployment("test-app", "default")
	dep.Spec.Replicas = int32Ptr(2)
	dep.Annotations[AnnotationDrainState] = string(DrainStateScaledUp)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationDrainNode] = "node-gone"
	dep.Annotations[AnnotationDrainStart] = time.Now().UTC().Format(time.RFC3339)

	r, c := newReconciler(dep)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-gone"}}

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var updatedDep appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "test-app", Namespace: "default"}, &updatedDep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if *updatedDep.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 after abort, got %d", *updatedDep.Spec.Replicas)
	}
	if _, exists := updatedDep.Annotations[AnnotationDrainState]; exists {
		t.Fatal("expected drain annotations to be cleared after abort")
	}
}
