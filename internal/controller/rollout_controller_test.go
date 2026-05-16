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

func restartTestConfig() *config.Config {
	return &config.Config{
		EnabledAnnotation:       AnnotationEnabled,
		RequeueInterval:         5 * time.Second,
		ReadinessTimeout:        10 * time.Minute,
		RestartSurgeEnabled:     true,
		RestartSurgeGracePeriod: 60 * time.Second,
		RestartSurgeTimeout:     10 * time.Minute,
	}
}

func restartTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = autoscalingv2.AddToScheme(s)
	_ = policyv1.AddToScheme(s)
	_ = rolloutsv1alpha1.AddToScheme(s)
	return s
}

func newRolloutReconciler(now time.Time, objs ...client.Object) (*RolloutReconciler, client.Client) {
	scheme := restartTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
	return &RolloutReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		Config:   restartTestConfig(),
		now:      func() time.Time { return now },
	}, c
}

func newRollout(name, namespace string) *rolloutsv1alpha1.Rollout {
	one := int32(1)
	return &rolloutsv1alpha1.Rollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 1,
			Annotations: map[string]string{
				AnnotationEnabled: "true",
			},
		},
		Spec: rolloutsv1alpha1.RolloutSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
			},
		},
		Status: rolloutsv1alpha1.RolloutStatus{
			Phase: rolloutsv1alpha1.RolloutPhaseHealthy,
		},
	}
}

// setRestartPending stamps the Rollout the way upstream Argo Rollouts does
// while a restart is pending: spec.RestartAt set, status.RestartedAt unset,
// phase flipped to Progressing with status.message. Defaults to the canonical
// "rollout is restarting" sentinel (see argoproj/argo-rollouts
// utils/rollout/rolloututil.go); pass a different message to simulate a
// concurrent user-driven deploy.
func setRestartPending(ro *rolloutsv1alpha1.Rollout, restartAt time.Time, msg ...string) {
	m := argoRestartingMessage
	if len(msg) > 0 {
		m = msg[0]
	}
	t := metav1.NewTime(restartAt)
	ro.Spec.RestartAt = &t
	ro.Status.Phase = rolloutsv1alpha1.RolloutPhaseProgressing
	ro.Status.Message = m
}

func newRolloutPod(name, namespace, appLabel string, created time.Time, ready bool) *corev1.Pod {
	conds := []corev1.PodCondition{}
	if ready {
		conds = append(conds, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			Labels:            map[string]string{"app": appLabel},
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: conds,
		},
	}
}

func newRolloutPDB(name, namespace string, appLabel string) *policyv1.PodDisruptionBudget {
	minAvail := intstr.FromInt(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvail,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": appLabel}},
		},
	}
}

// TestRestartSurge_NotOptedIn: workload without enabled annotation is ignored.
func TestRestartSurge_NotOptedIn(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	delete(ro.Annotations, AnnotationEnabled)
	setRestartPending(ro, now.Add(-5*time.Minute))
	pod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")

	r, c := newRolloutReconciler(now, ro, pod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, exists := got.Annotations[AnnotationRestartSurgeState]; exists {
		t.Fatal("expected no restart-surge state on non-opted-in workload")
	}
}

// TestRestartSurge_NoRestartAt: workload without spec.restartAt is ignored.
func TestRestartSurge_NoRestartAt(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	pod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")

	r, c := newRolloutReconciler(now, ro, pod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, exists := got.Annotations[AnnotationRestartSurgeState]; exists {
		t.Fatal("expected no restart-surge state when restartAt is unset")
	}
}

// TestRestartSurge_WithinGracePeriod: restartAt is recent (less than grace),
// so we requeue but do not act yet.
func TestRestartSurge_WithinGracePeriod(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-30*time.Second))
	pod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")

	r, c := newRolloutReconciler(now, ro, pod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected requeue during grace period")
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, exists := got.Annotations[AnnotationRestartSurgeState]; exists {
		t.Fatal("expected no restart-surge state within grace period")
	}
	if got.Spec.Replicas != nil && *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 (unchanged), got %d", *got.Spec.Replicas)
	}
}

// TestRestartSurge_NoPDB: no matching PDB, so Argo would not be blocked
// and we have nothing to fix.
func TestRestartSurge_NoPDB(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute))
	pod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)

	r, c := newRolloutReconciler(now, ro, pod)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, exists := got.Annotations[AnnotationRestartSurgeState]; exists {
		t.Fatal("expected no restart-surge state without a PDB")
	}
}

// TestRestartSurge_HappyPath drives the full state machine for a single-replica
// Rollout stuck on a PDB:
//
//	Pending → ScaledUp (replicas 1→2)
//	ScaledUp → still ScaledUp (waiting for ready)
//	ScaledUp → Ready (surge pod is Ready)
//	Ready → Draining (Argo finished: status.restartedAt == spec.restartAt; replicas 2→1)
//	Draining → Done
//	Done → cleared
func TestRestartSurge_HappyPath(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute))
	oldPod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")

	r, c := newRolloutReconciler(now, ro, oldPod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	// Step 1: trigger surge.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[AnnotationRestartSurgeState] != string(DrainStateScaledUp) {
		t.Fatalf("expected state=scaled-up, got %s", got.Annotations[AnnotationRestartSurgeState])
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 2 {
		t.Fatalf("expected replicas=2 after surge, got %v", got.Spec.Replicas)
	}
	if got.Annotations[AnnotationOriginalReplicas] != "1" {
		t.Fatalf("expected original-replicas=1, got %s", got.Annotations[AnnotationOriginalReplicas])
	}
	if got.Annotations[AnnotationRestartSurgeStart] == "" {
		t.Fatal("expected restart-surge-start to be set")
	}

	// Step 2: surge pod still starting → stay in scaled-up.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 2: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[AnnotationRestartSurgeState] != string(DrainStateScaledUp) {
		t.Fatalf("expected state=scaled-up (still waiting), got %s", got.Annotations[AnnotationRestartSurgeState])
	}

	// Step 3: create the surge pod, ready → transition to Ready.
	surgePod := newRolloutPod("app-new", "default", "app", now.Add(-1*time.Minute), true)
	if err := c.Create(ctx, surgePod); err != nil {
		t.Fatalf("create surge pod: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 3: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[AnnotationRestartSurgeState] != string(DrainStateReady) {
		t.Fatalf("expected state=ready, got %s", got.Annotations[AnnotationRestartSurgeState])
	}

	// Step 4: Argo evicts old pod and sets status.restartedAt → Draining.
	if err := c.Delete(ctx, oldPod); err != nil {
		t.Fatalf("delete old pod: %v", err)
	}
	got.Status.RestartedAt = ro.Spec.RestartAt.DeepCopy()
	if err := c.Update(ctx, &got); err != nil {
		t.Fatalf("update status: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 4: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[AnnotationRestartSurgeState] != string(DrainStateDraining) {
		t.Fatalf("expected state=draining, got %s", got.Annotations[AnnotationRestartSurgeState])
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 after scale-down, got %v", got.Spec.Replicas)
	}

	// Step 5: Draining → Done.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 5: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[AnnotationRestartSurgeState] != string(DrainStateDone) {
		t.Fatalf("expected state=done, got %s", got.Annotations[AnnotationRestartSurgeState])
	}

	// Step 6: cleanup.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("step 6: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeState]; ok {
		t.Fatal("expected restart-surge annotations cleared")
	}
	if _, ok := got.Annotations[AnnotationOriginalReplicas]; ok {
		t.Fatal("expected original-replicas cleared")
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeStart]; ok {
		t.Fatal("expected restart-surge-start cleared")
	}
	if got.Annotations[AnnotationEnabled] != "true" {
		t.Fatal("expected enabled annotation to remain")
	}
}

// TestRestartSurge_DrainTakesOver: if a drain-surge starts while a restart-surge
// is mid-operation, the restart-surge yields and aborts.
func TestRestartSurge_DrainTakesOver(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute))
	two := int32(2)
	ro.Spec.Replicas = &two
	ro.Annotations[AnnotationRestartSurgeState] = string(DrainStateScaledUp)
	ro.Annotations[AnnotationOriginalReplicas] = "1"
	ro.Annotations[AnnotationRestartSurgeStart] = now.Format(time.RFC3339)
	// Drain-surge starts and stamps its own state.
	ro.Annotations[AnnotationDrainState] = string(DrainStateScaledUp)
	ro.Annotations[AnnotationDrainNode] = "node-1"
	ro.Annotations[AnnotationDrainStart] = now.Format(time.RFC3339)

	r, c := newRolloutReconciler(now, ro)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeState]; ok {
		t.Fatal("expected restart-surge annotations cleared after drain takes over")
	}
	// Drain annotations should remain (cleared together by drain controller).
	if got.Annotations[AnnotationDrainState] == "" {
		t.Fatal("expected drain annotations to remain after restart-surge yields")
	}
	// Replicas should be restored to the original (1), since restart-surge owned the scaling.
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas restored to 1, got %v", got.Spec.Replicas)
	}
}

// TestRestartSurge_Timeout: a stale operation past the timeout is aborted.
func TestRestartSurge_Timeout(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-1*time.Hour))
	two := int32(2)
	ro.Spec.Replicas = &two
	ro.Annotations[AnnotationRestartSurgeState] = string(DrainStateScaledUp)
	ro.Annotations[AnnotationOriginalReplicas] = "1"
	// Start time well past timeout (10m default), but within 3x (so it is timeout, not stale).
	ro.Annotations[AnnotationRestartSurgeStart] = now.Add(-15 * time.Minute).Format(time.RFC3339)

	r, c := newRolloutReconciler(now, ro)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeState]; ok {
		t.Fatal("expected restart-surge annotations cleared after timeout abort")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas restored to 1 after abort, got %v", got.Spec.Replicas)
	}
}

// TestRestartSurge_HPA: when an HPA targets the Rollout, patch hpa.spec.minReplicas
// instead of rollout.spec.replicas.
func TestRestartSurge_HPA(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute))
	oldPod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")
	minReplicas := int32(1)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "app-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Rollout", Name: "app"},
			MinReplicas:    &minReplicas,
			MaxReplicas:    5,
		},
	}

	r, c := newRolloutReconciler(now, ro, oldPod, pdb, hpa)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected rollout replicas untouched (=1) when HPA present, got %v", got.Spec.Replicas)
	}
	if got.Annotations[AnnotationHPAName] != "app-hpa" {
		t.Fatalf("expected HPA name annotation, got %s", got.Annotations[AnnotationHPAName])
	}
	var gotHPA autoscalingv2.HorizontalPodAutoscaler
	if err := c.Get(ctx, types.NamespacedName{Name: "app-hpa", Namespace: "default"}, &gotHPA); err != nil {
		t.Fatalf("get hpa: %v", err)
	}
	if gotHPA.Spec.MinReplicas == nil || *gotHPA.Spec.MinReplicas != 2 {
		t.Fatalf("expected HPA minReplicas=2, got %v", gotHPA.Spec.MinReplicas)
	}
}

// TestRestartSurge_HPA_StaleRolloutCache: simulates the production race where
// the first reconcile patches the HPA and the Rollout, but the second
// reconcile fires before the watch refreshes the Rollout in the cache. The
// second reconcile sees a Rollout without restart-surge annotations and an
// HPA already at minReplicas=2 — it must not capture 2 as the "original"
// minReplicas (which would later be restored, leaving the HPA permanently
// scaled up).
func TestRestartSurge_HPA_StaleRolloutCache(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute))
	oldPod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")
	minReplicas := int32(1)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "app-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Rollout", Name: "app"},
			MinReplicas:    &minReplicas,
			MaxReplicas:    5,
		},
	}

	r, c := newRolloutReconciler(now, ro, oldPod, pdb, hpa)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	// First reconcile: should patch HPA min 1→2 and annotate Rollout.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	var afterFirst rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &afterFirst); err != nil {
		t.Fatalf("get rollout after 1: %v", err)
	}
	if afterFirst.Annotations[AnnotationHPAOriginalMinReplicas] != "1" {
		t.Fatalf("after reconcile 1: expected HPAOriginalMinReplicas=1, got %q", afterFirst.Annotations[AnnotationHPAOriginalMinReplicas])
	}

	// Simulate stale cache: roll the Rollout back to its pre-reconcile state
	// in the client (annotations cleared) while leaving the HPA at min=2 (the
	// API server already saw that patch). This is the exact window where a
	// reconcile can re-enter handleRestartPending and re-read the HPA.
	stale := ro.DeepCopy()
	stale.ResourceVersion = afterFirst.ResourceVersion
	if err := c.Update(ctx, stale); err != nil {
		t.Fatalf("rollback rollout: %v", err)
	}

	// Second reconcile: must defer (not write HPAOriginalMinReplicas=2 nor
	// re-patch the HPA). The HPA-already-at-target guard makes the handler
	// idempotent on stale-cache re-entry.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	var afterSecond rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &afterSecond); err != nil {
		t.Fatalf("get rollout after 2: %v", err)
	}
	// The re-entry guard must not write "2" (the post-surge min) as the new
	// baseline. Annotation is "" here because we explicitly rolled the
	// Rollout back to simulate the stale cache; in production the watch
	// would land with the real value ("1") soon after.
	if got := afterSecond.Annotations[AnnotationHPAOriginalMinReplicas]; got == "2" {
		t.Fatalf("HPA original-min poisoned by stale-cache re-entry: got %q (expected \"\" or \"1\", never \"2\")", got)
	}
	var hpaAfterSecond autoscalingv2.HorizontalPodAutoscaler
	if err := c.Get(ctx, types.NamespacedName{Name: "app-hpa", Namespace: "default"}, &hpaAfterSecond); err != nil {
		t.Fatalf("get hpa: %v", err)
	}
	if hpaAfterSecond.Spec.MinReplicas == nil || *hpaAfterSecond.Spec.MinReplicas != 2 {
		t.Fatalf("HPA minReplicas should remain at surge target (2), got %v", hpaAfterSecond.Spec.MinReplicas)
	}
}

// TestRestartSurge_HPAMaxTooLow: HPA with maxReplicas<2 prevents surge.
func TestRestartSurge_HPAMaxTooLow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute))
	oldPod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "app-hpa", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Rollout", Name: "app"},
			MaxReplicas:    1,
		},
	}

	r, c := newRolloutReconciler(now, ro, oldPod, pdb, hpa)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeState]; ok {
		t.Fatal("expected no restart-surge state when HPA maxReplicas is too low")
	}
}

// TestRestartSurge_ProgressingForOtherReason: a Rollout in Progressing for a
// reason other than a pending restart (e.g. a user-driven deploy in flight)
// must NOT be surged — we only act when status.message matches Argo's
// canonical "rollout is restarting" sentinel.
func TestRestartSurge_ProgressingForOtherReason(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	setRestartPending(ro, now.Add(-5*time.Minute), "more replicas need to be updated")
	oldPod := newRolloutPod("app-old", "default", "app", now.Add(-1*time.Hour), true)
	pdb := newRolloutPDB("app-pdb", "default", "app")

	r, c := newRolloutReconciler(now, ro, oldPod, pdb)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "app", Namespace: "default"}}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeState]; ok {
		t.Fatal("expected no restart-surge state when Rollout is Progressing for a non-restart reason")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas untouched (=1), got %v", got.Spec.Replicas)
	}
}

// TestRestartSurge_OrphanRecovery: a workload with restart-surge state but
// no remaining pending restart (status caught up) is cleaned up.
func TestRestartSurge_OrphanRecovery(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	ro := newRollout("app", "default")
	restartAt := metav1.NewTime(now.Add(-1 * time.Hour))
	ro.Spec.RestartAt = &restartAt
	ro.Status.RestartedAt = &restartAt // Argo finished
	two := int32(2)
	ro.Spec.Replicas = &two
	ro.Annotations[AnnotationRestartSurgeState] = string(DrainStateReady)
	ro.Annotations[AnnotationOriginalReplicas] = "1"
	ro.Annotations[AnnotationRestartSurgeStart] = now.Add(-5 * time.Minute).Format(time.RFC3339)

	r, c := newRolloutReconciler(now, ro)
	if err := r.RecoverOrphans(ctx); err != nil {
		t.Fatalf("recover orphans: %v", err)
	}
	var got rolloutsv1alpha1.Rollout
	if err := c.Get(ctx, types.NamespacedName{Name: "app", Namespace: "default"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[AnnotationRestartSurgeState]; ok {
		t.Fatal("expected restart-surge annotations cleared by orphan recovery")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("expected replicas=1 after orphan recovery, got %v", got.Spec.Replicas)
	}
}

// TestRestartSurge_NotFound: a missing Rollout reconcile is a no-op.
func TestRestartSurge_NotFound(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	r, _ := newRolloutReconciler(now)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("expected no error for missing rollout, got %v", err)
	}
}
