package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aeyrtonvs/k8s-drain-surge/internal/config"
)

// RolloutReconciler handles restart-surge: when Argo Rollouts'
// PodRestarter is blocked by a PDB on a single-replica workload, this
// reconciler surges the Rollout by +1 so the PDB allows eviction, then
// scales back once Argo has finished replacing the pod.
//
// Only Argo Rollouts are watched. Deployments do not need this protection:
// kubectl rollout restart on a Deployment changes spec.template (timestamp
// annotation) so the controller creates a new ReplicaSet via delete (not
// eviction) and PDBs are not consulted.
type RolloutReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Config   *config.Config

	// now is injectable for tests. Defaults to time.Now.
	now func() time.Time
}

func (r *RolloutReconciler) timeNow() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *RolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues(LogFieldRollout, req.Name, LogFieldNamespace, req.Namespace)
	ctx = log.IntoContext(ctx, logger)

	var ro rolloutsv1alpha1.Rollout
	if err := r.Get(ctx, req.NamespacedName, &ro); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	wl := &RolloutWorkload{Rollout: &ro}
	return r.reconcileWorkload(ctx, wl)
}

func (r *RolloutReconciler) reconcileWorkload(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	ctx = ctxWithWorkload(ctx, wl)
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	annotations := meta.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}

	currentState := DrainState(annotations[AnnotationRestartSurgeState])
	isActive := currentState != DrainStateNone && currentState != DrainStateDone

	// Stale/timeout abort, mirroring NodeReconciler.reconcileWorkload.
	if isActive {
		if start, ok := parseRestartSurgeStart(annotations); ok {
			elapsed := r.timeNow().Sub(start)
			if elapsed > 3*r.Config.RestartSurgeTimeout {
				logger.Info("stale restart-surge state detected, force aborting", LogFieldState, currentState)
				r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "RestartSurgeStale", "Force aborting stale restart-surge operation")
				return r.abortRestartSurge(ctx, wl)
			}
			if elapsed > r.Config.RestartSurgeTimeout {
				logger.Info("restart-surge operation timed out", LogFieldState, currentState)
				r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "RestartSurgeTimeout", "Restart-surge operation timed out")
				return r.abortRestartSurge(ctx, wl)
			}
		}
	}

	// If a drain is already in progress on this workload, defer entirely:
	// the drain state machine owns the surge during its window. Clear only
	// our exclusive annotations (preserve shared keys the drain relies on)
	// and restore replicas to the original — the drain controller will
	// re-apply +1 on its own pass.
	if annotations[AnnotationDrainState] != "" {
		if isActive {
			logger.Info("drain operation took over, yielding restart-surge")
			return r.yieldToDrain(ctx, wl)
		}
		return ctrl.Result{}, nil
	}

	switch currentState {
	case DrainStateNone:
		return r.handleRestartPending(ctx, wl)
	case DrainStatePending:
		return r.handleRestartScaleUp(ctx, wl)
	case DrainStateScaledUp:
		return r.handleRestartWaitReady(ctx, wl)
	case DrainStateReady:
		return r.handleRestartWaitForArgo(ctx, wl)
	case DrainStateDraining:
		return r.handleRestartScaleDown(ctx, wl)
	case DrainStateDone:
		return r.handleRestartCleanup(ctx, wl)
	default:
		logger.Info("unknown restart-surge state, aborting", LogFieldState, currentState)
		return r.abortRestartSurge(ctx, wl)
	}
}

func (r *RolloutReconciler) handleRestartPending(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if meta.Annotations[r.Config.EnabledAnnotation] != "true" {
		return ctrl.Result{}, nil
	}
	if !wl.CanSurge() {
		return ctrl.Result{}, nil
	}
	// Multi-replica restarts are not blocked by `minAvailable: 1` PDBs; Argo
	// progresses unaided.
	if wl.GetReplicas() != 1 {
		return ctrl.Result{}, nil
	}

	pods, err := listMatchingPods(ctx, r.Client, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}

	restartPending := wl.Rollout.Spec.RestartAt != nil &&
		!wl.Rollout.Status.RestartedAt.Equal(wl.Rollout.Spec.RestartAt)

	if !isRestartStuck(wl.Rollout, pods, r.Config.RestartSurgeGracePeriod, r.timeNow()) {
		if !restartPending {
			return ctrl.Result{}, nil
		}
		restartAt := wl.Rollout.Spec.RestartAt.Time
		restartAtRFC := restartAt.Format(time.RFC3339)
		elapsed := r.timeNow().Sub(restartAt)
		remaining := r.Config.RestartSurgeGracePeriod - elapsed

		if remaining > 0 {
			// Emit one Info per restartAt observed so operators see the
			// controller acknowledged the event without flooding logs on
			// each requeue. The annotation tracks which restartAt we've
			// already logged; patchReplicasAndAnnotations clears it on the
			// next patch (e.g. when we transition to scaled-up) because the
			// merge nullifies any drain annotation key not in the new patch.
			if meta.Annotations[AnnotationRestartSurgePendingLogged] != restartAtRFC {
				logger.Info("restart pending, waiting for grace period before triggering surge",
					LogFieldRestartAt, restartAt,
					LogFieldElapsed, elapsed.Round(time.Second),
					LogFieldGracePeriod, r.Config.RestartSurgeGracePeriod,
					LogFieldRemaining, remaining.Round(time.Second),
				)
				if meta.Annotations == nil {
					meta.Annotations = make(map[string]string)
				}
				meta.Annotations[AnnotationRestartSurgePendingLogged] = restartAtRFC
				if err := wl.Patch(ctx, r.Client); err != nil {
					return ctrl.Result{}, fmt.Errorf("stamp pending-logged annotation: %w", err)
				}
			}
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
		// Grace already elapsed but isRestartStuck is false → no pods still
		// predate restartAt → Argo's PodRestarter finished without us.
		logger.Info("restart pending but no stale pods remain, Argo handled it unaided",
			LogFieldRestartAt, restartAt,
		)
		return ctrl.Result{}, nil
	}

	if !wl.IsStableForRestart() {
		phase, msg := wl.Rollout.Status.Phase, wl.Rollout.Status.Message
		logger.V(1).Info("rollout not stable for restart-surge, skipping", LogFieldRolloutPhase, phase, LogFieldRolloutMessage, msg)
		// Keep the event message stable so EventBroadcaster can aggregate
		// repeats: phase/message change during a deploy and would otherwise
		// produce one new event per requeue.
		r.Recorder.Event(wl.Object(), corev1.EventTypeWarning, "RestartSurgeSkipped", "Rollout phase is not a pending restart — skipping restart-surge")
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	hasPDB, err := FindMatchingPDB(ctx, r.Client, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}
	if !hasPDB {
		// Without a PDB Argo's eviction would have succeeded on its own.
		return ctrl.Result{}, nil
	}

	originalReplicas := wl.GetReplicas()
	if meta.Annotations == nil {
		meta.Annotations = make(map[string]string)
	}
	meta.Annotations[AnnotationRestartSurgeState] = string(DrainStateScaledUp)
	meta.Annotations[AnnotationOriginalReplicas] = strconv.Itoa(int(originalReplicas))
	meta.Annotations[AnnotationRestartSurgeStart] = r.timeNow().UTC().Format(time.RFC3339)

	hpa, err := FindMatchingHPA(ctx, r.Client, meta.Namespace, meta.Name, wl.GetObjectKind())
	if err != nil {
		return ctrl.Result{}, err
	}

	if hpa != nil {
		if hpa.Spec.MaxReplicas < originalReplicas+1 {
			logger.Info("HPA maxReplicas too low for restart-surge, skipping", LogFieldMaxReplicas, hpa.Spec.MaxReplicas)
			r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "RestartSurgeSkipped", "HPA maxReplicas=%d prevents restart-surge", hpa.Spec.MaxReplicas)
			return ctrl.Result{}, nil
		}
		// Re-entry guard: if the HPA is already at the surge target, a prior
		// reconcile already patched it — we are seeing a stale Rollout cache.
		// Treat the current minReplicas as our work-in-progress, not as
		// "original", and requeue quickly so the next reconcile sees the
		// propagated Rollout annotations (cache typically catches up in ms).
		if IsHPAAtMinReplicas(hpa, originalReplicas+1) {
			logger.V(1).Info("HPA already at surge target, deferring to next reconcile", LogFieldHPA, hpa.Name, LogFieldMinReplicas, *hpa.Spec.MinReplicas)
			return ctrl.Result{RequeueAfter: 250 * time.Millisecond}, nil
		}
		originalMin := HPAMinReplicasOrDefault(hpa)
		meta.Annotations[AnnotationHPAName] = hpa.Name
		meta.Annotations[AnnotationHPAOriginalMinReplicas] = strconv.Itoa(int(originalMin))

		// Persist annotations BEFORE patching the HPA: if the HPA patch fails
		// or the next reconcile fires on a stale Rollout cache, the Rollout
		// annotations are the source of truth for HPAOriginalMinReplicas. The
		// HPA-already-at-target guard above handles the inverse case.
		if err := wl.Patch(ctx, r.Client); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch rollout for restart-surge scale-up: %w", err)
		}
		if err := PatchHPAMinReplicas(ctx, r.Client, meta.Namespace, hpa.Name, originalReplicas+1); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch HPA minReplicas for restart-surge: %w", err)
		}
		logger.Info("patched HPA minReplicas for restart-surge", LogFieldHPA, hpa.Name, LogFieldFrom, originalMin, LogFieldTo, originalReplicas+1)
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "RestartSurgeStart", "Patched HPA %s minReplicas from %d to %d to unblock restart", hpa.Name, originalMin, originalReplicas+1)
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	wl.SetReplicas(originalReplicas + 1)
	logger.Info("scaled up rollout for restart-surge", LogFieldFrom, originalReplicas, LogFieldTo, originalReplicas+1, LogFieldRestartAt, wl.Rollout.Spec.RestartAt.Time)
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "RestartSurgeStart", "Restart blocked by PDB, scaled from %d to %d", originalReplicas, originalReplicas+1)

	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch rollout for restart-surge scale-up: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleRestartScaleUp re-applies the scale-up if a competing controller
// (ArgoCD, etc.) reset spec.replicas during the operation. Skips the patch
// entirely when replicas are still correct.
func (r *RolloutReconciler) handleRestartScaleUp(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		logger.Error(err, "cannot determine original replicas, aborting restart-surge")
		return r.abortRestartSurge(ctx, wl)
	}

	if meta.Annotations[AnnotationHPAName] != "" || wl.GetReplicas() > original {
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	logger.Info("replicas were reset, competing controller detected — re-applying restart-surge scale-up")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "CompetingController", "Replicas were reset externally during restart-surge")
	wl.SetReplicas(original + 1)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-apply restart-surge scale-up: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleRestartWaitReady waits until at least 2 pods (selector-matched, not
// terminating) are Ready: the original plus the surge replica. Once both are
// Ready, the PDB will permit Argo's PodRestarter to evict the old pod.
func (r *RolloutReconciler) handleRestartWaitReady(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if meta.Annotations[AnnotationHPAName] == "" {
		original, err := getOriginalReplicas(meta.Annotations)
		if err != nil {
			logger.Error(err, "cannot determine original replicas, aborting restart-surge")
			return r.abortRestartSurge(ctx, wl)
		}
		if wl.GetReplicas() <= original {
			logger.Info("replicas were reset externally during wait-ready, re-applying restart-surge scale-up")
			wl.SetReplicas(original + 1)
			if err := wl.Patch(ctx, r.Client); err != nil {
				return ctrl.Result{}, fmt.Errorf("re-apply restart-surge scale-up: %w", err)
			}
			return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
		}
	}

	pods, err := listMatchingPods(ctx, r.Client, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}

	readyCount := 0
	for i := range pods {
		p := &pods[i]
		if isPodTerminating(p) {
			continue
		}
		if isPodReady(p) {
			readyCount++
		}
	}

	if readyCount >= 2 {
		logger.Info("surge replica ready, allowing Argo PodRestarter to proceed", LogFieldReplicas, readyCount)
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "RestartSurgeReady", "Surge replica ready; Argo PodRestarter can now evict the stale pod")
		meta.Annotations[AnnotationRestartSurgeState] = string(DrainStateReady)
		if err := wl.Patch(ctx, r.Client); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch rollout to ready: %w", err)
		}
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	logger.V(1).Info("waiting for surge replica to become ready", LogFieldReplicas, readyCount)
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleRestartWaitForArgo waits for Argo's PodRestarter to evict the old
// pod and the Rollout controller to recreate it. Detection: either
// status.restartedAt catches up to spec.restartAt, or no remaining pods
// predate spec.restartAt.
func (r *RolloutReconciler) handleRestartWaitForArgo(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	// Fast path: Argo already reported completion. Skip the pod list entirely.
	statusCaughtUp := wl.Rollout.Spec.RestartAt != nil &&
		wl.Rollout.Status.RestartedAt.Equal(wl.Rollout.Spec.RestartAt)

	if !statusCaughtUp {
		pods, err := listMatchingPods(ctx, r.Client, meta.Namespace, wl.GetPodSelector())
		if err != nil {
			return ctrl.Result{}, err
		}
		if !restartCompletedByArgo(wl.Rollout, pods) {
			logger.V(1).Info("waiting for Argo PodRestarter to evict stale pod")
			return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
		}
	}

	if err := r.restoreHPA(ctx, meta); err != nil {
		return ctrl.Result{}, fmt.Errorf("restore HPA after restart: %w", err)
	}

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		logger.Error(err, "cannot determine original replicas, aborting restart-surge")
		return r.abortRestartSurge(ctx, wl)
	}

	if meta.Annotations[AnnotationHPAName] == "" {
		wl.SetReplicas(original)
	}
	meta.Annotations[AnnotationRestartSurgeState] = string(DrainStateDraining)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch rollout for restart-surge scale-down: %w", err)
	}
	logger.Info("Argo PodRestarter completed restart, scaling down")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "RestartSurgeScaleDown", "Restart completed, scaled down to %d", original)
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *RolloutReconciler) handleRestartScaleDown(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		logger.Error(err, "cannot determine original replicas, aborting restart-surge")
		return r.abortRestartSurge(ctx, wl)
	}
	if meta.Annotations[AnnotationHPAName] == "" && wl.GetReplicas() != original {
		wl.SetReplicas(original)
	}
	meta.Annotations[AnnotationRestartSurgeState] = string(DrainStateDone)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch rollout to done: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *RolloutReconciler) handleRestartCleanup(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	clearRestartSurgeAnnotations(meta.Annotations)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch rollout for restart-surge cleanup: %w", err)
	}
	logger.Info("restart-surge operation completed")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "RestartSurgeComplete", "Restart-surge operation completed successfully")
	return ctrl.Result{}, nil
}

func (r *RolloutReconciler) abortRestartSurge(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if err := r.restoreHPA(ctx, meta); err != nil {
		logger.Error(err, "failed to restore HPA during restart-surge abort")
	}
	if original, err := getOriginalReplicas(meta.Annotations); err == nil {
		wl.SetReplicas(original)
	}
	clearRestartSurgeAnnotations(meta.Annotations)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("abort restart-surge: %w", err)
	}
	logger.Info("aborted restart-surge operation")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "RestartSurgeAborted", "Restart-surge operation aborted")
	return ctrl.Result{}, nil
}

// yieldToDrain restores replicas to the original (the drain controller will
// re-surge from its own pass) and clears only the restart-surge exclusive
// annotations so the drain's shared bookkeeping (original-replicas, HPA) is
// preserved.
func (r *RolloutReconciler) yieldToDrain(ctx context.Context, wl *RolloutWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if meta.Annotations[AnnotationHPAName] == "" {
		if original, err := getOriginalReplicas(meta.Annotations); err == nil {
			wl.SetReplicas(original)
		}
	}
	clearRestartSurgeExclusiveAnnotations(meta.Annotations)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("yield to drain: %w", err)
	}
	logger.Info("yielded restart-surge to drain")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "RestartSurgeYielded", "Restart-surge yielded to in-progress drain")
	return ctrl.Result{}, nil
}

// restoreHPA mirrors NodeReconciler.restoreHPA.
func (r *RolloutReconciler) restoreHPA(ctx context.Context, meta *metav1.ObjectMeta) error {
	hpaName := meta.Annotations[AnnotationHPAName]
	if hpaName == "" {
		return nil
	}
	originalMinStr := meta.Annotations[AnnotationHPAOriginalMinReplicas]
	originalMin, err := strconv.Atoi(originalMinStr)
	if err != nil {
		return fmt.Errorf("invalid %s value %q: %w", AnnotationHPAOriginalMinReplicas, originalMinStr, err)
	}
	logger := log.FromContext(ctx)
	logger.Info("restoring HPA minReplicas", LogFieldHPA, hpaName, LogFieldMinReplicas, originalMin)
	return PatchHPAMinReplicas(ctx, r.Client, meta.Namespace, hpaName, int32(originalMin))
}

// RecoverOrphans aborts restart-surge operations whose Rollout no longer has
// a pending restart (spec.restartAt cleared, or status.restartedAt caught up).
// Called once on leader election.
func (r *RolloutReconciler) RecoverOrphans(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("restart-surge-orphan-recovery")
	logger.Info("starting restart-surge orphan recovery scan")

	var roList rolloutsv1alpha1.RolloutList
	if err := r.List(ctx, &roList); err != nil {
		return fmt.Errorf("list rollouts: %w", err)
	}
	for i := range roList.Items {
		ro := &roList.Items[i]
		state := DrainState(ro.Annotations[AnnotationRestartSurgeState])
		if state == DrainStateNone {
			continue
		}
		// Argo finished or operator cleared restartAt → our work is done,
		// scale back and clean up.
		argoIdle := ro.Spec.RestartAt == nil ||
			(ro.Status.RestartedAt != nil && ro.Status.RestartedAt.Equal(ro.Spec.RestartAt))
		if !argoIdle {
			continue
		}
		logger.Info("found orphaned restart-surge", LogFieldRollout, ro.Name, LogFieldNamespace, ro.Namespace, LogFieldState, state)
		if _, err := r.abortRestartSurge(ctx, &RolloutWorkload{Rollout: ro}); err != nil {
			logger.Error(err, "failed to abort orphaned restart-surge", LogFieldRollout, ro.Name)
		}
	}
	logger.Info("restart-surge orphan recovery scan complete")
	return nil
}

func (r *RolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rolloutsv1alpha1.Rollout{}, builder.WithPredicates(rolloutRelevantPredicate(r.Config.EnabledAnnotation))).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPodToRollout)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		Complete(r)
}

// rolloutRelevantPredicate filters Rollout events to those the reconciler
// can act on: workloads already mid-operation (we must observe progress to
// finish cleanup) OR opted-in workloads with a pending restart. This cuts
// reconciles for every unrelated Rollout in the cluster (ArgoCD-managed
// apps that never use restart-surge).
func rolloutRelevantPredicate(enabledAnnotation string) predicate.Predicate {
	relevant := func(o client.Object) bool {
		ann := o.GetAnnotations()
		if ann[AnnotationRestartSurgeState] != "" {
			return true
		}
		if ann[enabledAnnotation] != "true" {
			return false
		}
		ro, ok := o.(*rolloutsv1alpha1.Rollout)
		if !ok {
			return false
		}
		if ro.Spec.RestartAt == nil {
			return false
		}
		return !ro.Status.RestartedAt.Equal(ro.Spec.RestartAt)
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return relevant(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return relevant(e.ObjectNew) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return relevant(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return relevant(e.Object) },
	}
}

func (r *RolloutReconciler) mapPodToRollout(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	wl, err := ResolveWorkloadFromPod(ctx, r.Client, pod)
	if err != nil || wl == nil {
		return nil
	}
	ro, ok := wl.(*RolloutWorkload)
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: ro.Rollout.Name, Namespace: ro.Rollout.Namespace}}}
}
