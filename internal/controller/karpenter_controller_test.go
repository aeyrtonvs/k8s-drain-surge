package controller

import (
	"context"
	"testing"
	"time"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
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

	"github.com/aeyrtonvs/k8s-drain-surge/internal/config"
)

func karpenterTestConfig() *config.Config {
	return &config.Config{
		DrainTaints:               []config.DrainTaint{{Key: "karpenter.sh/disrupted", Effect: "NoSchedule"}, {Key: "node.kubernetes.io/unschedulable", Effect: "NoSchedule"}},
		EnabledAnnotation:         AnnotationEnabled,
		RequeueInterval:           5 * time.Second,
		ReadinessTimeout:          10 * time.Minute,
		KarpenterSurgeEnabled:     true,
		KarpenterSurgeGracePeriod: 60 * time.Second,
		KarpenterSurgeTimeout:     10 * time.Minute,
		KarpenterSurgeScanPeriod:  60 * time.Second,
	}
}

func karpenterTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = autoscalingv2.AddToScheme(s)
	_ = policyv1.AddToScheme(s)
	_ = rolloutsv1alpha1.AddToScheme(s)
	return s
}

func newKarpenterReconciler(now time.Time, objs ...client.Object) (*KarpenterSurgeReconciler, client.Client) {
	scheme := karpenterTestScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &KarpenterSurgeReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		Config:   karpenterTestConfig(),
		now:      func() time.Time { return now },
	}, c
}

func newKarpenterDeployment(name, namespace string, replicas int32) *appsv1.Deployment {
	r := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Generation:  1,
			Annotations: map[string]string{AnnotationEnabled: "true"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &r,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Conditions: []appsv1.DeploymentCondition{
				{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: reasonNewRSAvailable},
			},
		},
	}
}

func newKarpenterPDB(name, namespace, appLabel string, disruptionsAllowed int32) *policyv1.PodDisruptionBudget {
	minAvail := intstr.FromInt(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("pdb-" + name)},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvail,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": appLabel}},
		},
		Status: policyv1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: disruptionsAllowed,
			ExpectedPods:       1,
		},
	}
}

func newKarpenterPod(name, namespace, appLabel, nodeName string, ready bool) *corev1.Pod {
	conds := []corev1.PodCondition{}
	if ready {
		conds = append(conds, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": appLabel},
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: conds},
	}
}

func newCleanNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func newKarpenterHPA(name, namespace, targetName string, minR, maxR int32) *autoscalingv2.HorizontalPodAutoscaler {
	min := minR
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: &min,
			MaxReplicas: maxR,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       targetName,
			},
		},
	}
}

func mustGetDeployment(t *testing.T, ctx context.Context, c client.Client, ns, name string) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &d); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	return &d
}

// TestKarpenterSurge_NotOptedIn: missing the opt-in annotation is a no-op.
func TestKarpenterSurge_NotOptedIn(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	delete(dep.Annotations, AnnotationEnabled)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newCleanNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pdb, pod, node)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no karpenter-surge state on non-opted-in workload")
	}
}

// TestKarpenterSurge_NoPDB: opted-in but no matching PDB → no action.
func TestKarpenterSurge_NoPDB(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newCleanNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pod, node)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no state when no PDB matches")
	}
}

// TestKarpenterSurge_GracePeriod: first observation of disruptionsAllowed=0
// must NOT trigger a surge; the grace period needs to elapse.
func TestKarpenterSurge_GracePeriod(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newCleanNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pdb, pod, node)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	// First reconcile: records first-seen, returns RequeueAfter (no surge).
	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected requeue while waiting for grace period")
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no state during grace period")
	}

	// Advance time past grace; same PDB still blocked → surge fires.
	r.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile after grace: %v", err)
	}
	got = mustGetDeployment(t, ctx, c, "default", "app")
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateScaledUp) {
		t.Fatalf("expected state=scaled-up after grace, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 2 {
		t.Fatalf("expected replicas=2 after surge, got %v", got.Spec.Replicas)
	}
	if got.Annotations[AnnotationOriginalReplicas] != "1" {
		t.Fatalf("expected original-replicas=1, got %s", got.Annotations[AnnotationOriginalReplicas])
	}
	if got.Annotations[AnnotationKarpenterSurgePDB] != "default/app-pdb" {
		t.Fatalf("expected PDB annotation, got %s", got.Annotations[AnnotationKarpenterSurgePDB])
	}
}

// TestKarpenterSurge_HappyPath_Eviction drives the full state machine where
// Karpenter never applies a taint; instead the PDB recovers (pod predecessor
// is evicted) and the controller scales back.
func TestKarpenterSurge_HappyPath_Eviction(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	oldPod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newCleanNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pdb, oldPod, node)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	// Step 0: prime grace tracker (records first-seen now, returns no-op).
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	// Step 1: grace elapsed → scale up.
	r.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateScaledUp) {
		t.Fatalf("expected scaled-up, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}

	// Step 2: surge pod not Ready yet → stays scaled-up.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 2: %v", err)
	}
	got = mustGetDeployment(t, ctx, c, "default", "app")
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateScaledUp) {
		t.Fatalf("expected still scaled-up, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}

	// Step 3: create the new pod Ready on another node → Ready.
	node2 := newCleanNode("node-2")
	if err := c.Create(ctx, node2); err != nil {
		t.Fatalf("create node2: %v", err)
	}
	surgePod := newKarpenterPod("app-new", "default", "app", "node-2", true)
	if err := c.Create(ctx, surgePod); err != nil {
		t.Fatalf("create surge pod: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 3: %v", err)
	}
	got = mustGetDeployment(t, ctx, c, "default", "app")
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateReady) {
		t.Fatalf("expected ready, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}

	// Step 4: PDB recovers (Karpenter evicted the original pod) → Draining.
	var pdbObj policyv1.PodDisruptionBudget
	if err := c.Get(ctx, types.NamespacedName{Name: "app-pdb", Namespace: "default"}, &pdbObj); err != nil {
		t.Fatalf("get pdb: %v", err)
	}
	pdbObj.Status.DisruptionsAllowed = 1
	if err := c.Status().Update(ctx, &pdbObj); err != nil {
		t.Fatalf("update pdb status: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 4: %v", err)
	}
	got = mustGetDeployment(t, ctx, c, "default", "app")
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateDraining) {
		t.Fatalf("expected draining, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}

	// Step 5: Draining → scale back to original (1) → Done.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 5: %v", err)
	}
	got = mustGetDeployment(t, ctx, c, "default", "app")
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateDone) {
		t.Fatalf("expected done, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 after scale-down, got %v", got.Spec.Replicas)
	}

	// Step 6: cleanup.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 6: %v", err)
	}
	got = mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected annotations cleared")
	}
	if _, ok := got.Annotations[AnnotationOriginalReplicas]; ok {
		t.Fatal("expected original-replicas cleared")
	}
	if got.Annotations[AnnotationEnabled] != "true" {
		t.Fatal("expected enabled annotation to remain")
	}
}

// TestKarpenterSurge_DrainTaintYield: a taint appears mid-surge → yield.
func TestKarpenterSurge_DrainTaintYield(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateReady)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	// Node now has a drain taint.
	node := newTaintedNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pdb, pod, node)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatalf("expected karpenter-surge exclusive annotations cleared on yield, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}
	if got.Annotations[AnnotationOriginalReplicas] != "1" {
		t.Fatalf("expected shared annotation preserved for drain machinery, got %s", got.Annotations[AnnotationOriginalReplicas])
	}
}

// TestKarpenterSurge_DrainOwnsWorkload: another drain operation already owns the
// workload (AnnotationDrainState set) — the karpenter reconciler steps aside
// without touching anything.
func TestKarpenterSurge_DrainOwnsWorkload(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	dep.Annotations[AnnotationDrainState] = string(DrainStateScaledUp)
	dep.Annotations[AnnotationDrainNode] = "node-1"
	dep.Annotations[AnnotationDrainStart] = now.Format(time.RFC3339)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)

	r, c := newKarpenterReconciler(now, dep, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no karpenter-surge state when drain owns the workload")
	}
}

// TestKarpenterSurge_Timeout: operation older than KarpenterSurgeTimeout aborts.
func TestKarpenterSurge_Timeout(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateReady)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Add(-30 * time.Minute).Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"

	r, c := newKarpenterReconciler(now, dep)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected annotations cleared after timeout abort")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas restored to 1, got %v", got.Spec.Replicas)
	}
}

// TestKarpenterSurge_R9_ExternalScaleChange_NoHPA: while draining, operator
// scaled to 3 → controller yields without scaling back to 1.
func TestKarpenterSurge_R9_ExternalScaleChange_NoHPA(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 3) // operator moved to 3
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateDraining)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"

	r, c := newKarpenterReconciler(now, dep)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 3 {
		t.Fatalf("expected operator's 3 replicas preserved, got %v", got.Spec.Replicas)
	}
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected annotations cleared after yield")
	}
}

// TestKarpenterSurge_R9_ExternalScaleChange_HPA: while draining with HPA in
// play, the HPA minReplicas was changed externally → yield without restoring.
func TestKarpenterSurge_R9_ExternalScaleChange_HPA(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateDraining)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationHPAName] = "app-hpa"
	dep.Annotations[AnnotationHPAOriginalMinReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"
	// Operator moved HPA minReplicas to 5 (not 2 = original+1).
	hpa := newKarpenterHPA("app-hpa", "default", "app", 5, 10)

	r, c := newKarpenterReconciler(now, dep, hpa)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected annotations cleared after yield")
	}
	var gotHPA autoscalingv2.HorizontalPodAutoscaler
	if err := c.Get(ctx, types.NamespacedName{Name: "app-hpa", Namespace: "default"}, &gotHPA); err != nil {
		t.Fatalf("get hpa: %v", err)
	}
	if gotHPA.Spec.MinReplicas == nil || *gotHPA.Spec.MinReplicas != 5 {
		t.Fatalf("expected HPA minReplicas preserved at 5, got %v", gotHPA.Spec.MinReplicas)
	}
}

// TestKarpenterSurge_CaseD_HPAMaxReplicasOne: HPA caps at 1 → skip with reason.
func TestKarpenterSurge_CaseD_HPAMaxReplicasOne(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newCleanNode("node-1")
	hpa := newKarpenterHPA("app-hpa", "default", "app", 1, 1)

	r, c := newKarpenterReconciler(now, dep, pdb, pod, node, hpa)
	r.now = func() time.Time { return now.Add(2 * time.Minute) } // past grace
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no state when HPA maxReplicas=1")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas unchanged at 1, got %v", got.Spec.Replicas)
	}
}

// TestKarpenterSurge_CaseF_MultiReplica: replicas=2 → gate 5 (single-replica)
// rejects without action.
func TestKarpenterSurge_CaseF_MultiReplica(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)

	r, c := newKarpenterReconciler(now, dep, pdb)
	r.now = func() time.Time { return now.Add(2 * time.Minute) }
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no state for multi-replica workload")
	}
}

// TestKarpenterSurge_CaseG_OverConstrainedPDB: minAvailable=100% — surge would
// never resolve the block.
func TestKarpenterSurge_CaseG_OverConstrainedPDB(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	pct := intstr.FromString("100%")
	pdb.Spec.MinAvailable = &pct
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newCleanNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pdb, pod, node)
	r.now = func() time.Time { return now.Add(2 * time.Minute) }
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no state when PDB is over-constrained")
	}
}

// TestKarpenterSurge_PreExistingTaint: if the workload pod's node already has
// a drain taint when the PDB blocks, the karpenter reconciler defers without
// stamping any state (NodeReconciler owns this case).
func TestKarpenterSurge_PreExistingTaint(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1)
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)
	pod := newKarpenterPod("app-old", "default", "app", "node-1", true)
	node := newTaintedNode("node-1")

	r, c := newKarpenterReconciler(now, dep, pdb, pod, node)
	r.now = func() time.Time { return now.Add(2 * time.Minute) }
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected no state when node already has a drain taint")
	}
}

// TestKarpenterSurge_CompetingController: while in scaled-up, replicas were
// reset by ArgoCD back to 1 → re-apply the surge.
func TestKarpenterSurge_CompetingController(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 1) // ArgoCD reset back to 1
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateScaledUp)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"

	r, c := newKarpenterReconciler(now, dep)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 2 {
		t.Fatalf("expected re-applied replicas=2, got %v", got.Spec.Replicas)
	}
	if got.Annotations[AnnotationKarpenterSurgeState] != string(DrainStateScaledUp) {
		t.Fatalf("expected state still scaled-up, got %s", got.Annotations[AnnotationKarpenterSurgeState])
	}
}

// TestKarpenterSurge_OrphanRecovery: feature gate is off → abort active surge.
func TestKarpenterSurge_OrphanRecovery_FeatureGateOff(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateReady)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"
	pdb := newKarpenterPDB("app-pdb", "default", "app", 0)

	r, c := newKarpenterReconciler(now, dep, pdb)
	r.Config.KarpenterSurgeEnabled = false

	if err := r.RecoverOrphans(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected orphan annotations cleared when feature gate is off")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas restored to 1, got %v", got.Spec.Replicas)
	}
}

// TestKarpenterSurge_OrphanRecovery_PDBRecovered: PDB unblocked while
// controller was down → abort and restore.
func TestKarpenterSurge_OrphanRecovery_PDBRecovered(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	dep := newKarpenterDeployment("app", "default", 2)
	dep.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateReady)
	dep.Annotations[AnnotationKarpenterSurgeStart] = now.Format(time.RFC3339)
	dep.Annotations[AnnotationOriginalReplicas] = "1"
	dep.Annotations[AnnotationKarpenterSurgePDB] = "default/app-pdb"
	pdb := newKarpenterPDB("app-pdb", "default", "app", 1) // unblocked

	r, c := newKarpenterReconciler(now, dep, pdb)
	if err := r.RecoverOrphans(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	got := mustGetDeployment(t, ctx, c, "default", "app")
	if _, ok := got.Annotations[AnnotationKarpenterSurgeState]; ok {
		t.Fatal("expected orphan annotations cleared when PDB recovered")
	}
}
