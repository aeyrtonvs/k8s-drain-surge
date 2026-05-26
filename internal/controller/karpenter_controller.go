package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/aeyrtonvs/k8s-drain-surge/internal/config"
)

// KarpenterSurgeReconciler handles the pre-taint surge for the case where
// Karpenter's disruption controller refuses to taint a node because a PDB
// would block eviction in dry-run. The trigger is the PDB itself
// (disruptionsAllowed=0), not a Karpenter Event — see docs/specs/
// plan-karpenter-pretaint-surge.md for the rationale.
type KarpenterSurgeReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Config   *config.Config

	// RolloutsAvailable is set by main when the Argo Rollouts CRD is
	// present in the target cluster. When false, the reconciler skips the
	// Rollout watch, Rollout Gets in Reconcile, and Rollout List in
	// RecoverOrphans — otherwise the informer would fail to start and
	// crash the manager on Deployment-only clusters.
	RolloutsAvailable bool

	// grace tracks per-PDB first-seen times so a transient block does not
	// trigger an immediate surge. In-memory; rebuilt on restart.
	grace *gracePeriodTracker

	// now is injectable for tests.
	now func() time.Time
}

func (r *KarpenterSurgeReconciler) timeNow() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *KarpenterSurgeReconciler) tracker() *gracePeriodTracker {
	if r.grace == nil {
		r.grace = newGracePeriodTracker()
	}
	return r.grace
}

// Reconcile dispatches the request to the right workload kind. A
// reconcile.Request only carries NamespacedName, so we may have either a
// Rollout or a Deployment (or, rarely, both) sharing that name. We try both
// and prefer whichever has karpenter-surge bookkeeping or opt-in active. If
// the Argo Rollouts CRD is absent (RolloutsAvailable=false), only Deployments
// are considered.
func (r *KarpenterSurgeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues(LogFieldNamespace, req.Namespace, LogFieldWorkload, req.Name)
	ctx = log.IntoContext(ctx, logger)

	var (
		ro    *rolloutsv1alpha1.Rollout
		dep   *appsv1.Deployment
	)

	if r.RolloutsAvailable {
		var x rolloutsv1alpha1.Rollout
		err := r.Get(ctx, req.NamespacedName, &x)
		if err == nil {
			ro = &x
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	var y appsv1.Deployment
	err := r.Get(ctx, req.NamespacedName, &y)
	if err == nil {
		dep = &y
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if ro == nil && dep == nil {
		return ctrl.Result{}, nil
	}
	if ro != nil && dep != nil {
		// Same name, both kinds exist — pick the one with karpenter-surge
		// already in-flight, else the opted-in one, else log and prefer
		// the Rollout (existing behavior).
		switch {
		case ro.Annotations[AnnotationKarpenterSurgeState] != "":
			return r.reconcileWorkload(ctx, &RolloutWorkload{Rollout: ro})
		case dep.Annotations[AnnotationKarpenterSurgeState] != "":
			return r.reconcileWorkload(ctx, &DeploymentWorkload{Deployment: dep})
		case ro.Annotations[r.Config.EnabledAnnotation] == "true" && dep.Annotations[r.Config.EnabledAnnotation] != "true":
			return r.reconcileWorkload(ctx, &RolloutWorkload{Rollout: ro})
		case dep.Annotations[r.Config.EnabledAnnotation] == "true" && ro.Annotations[r.Config.EnabledAnnotation] != "true":
			return r.reconcileWorkload(ctx, &DeploymentWorkload{Deployment: dep})
		default:
			logger.Info("ambiguous workload: Rollout and Deployment with same NamespacedName both opted-in (or neither); preferring Rollout — rename one to disambiguate")
			return r.reconcileWorkload(ctx, &RolloutWorkload{Rollout: ro})
		}
	}
	if ro != nil {
		return r.reconcileWorkload(ctx, &RolloutWorkload{Rollout: ro})
	}
	return r.reconcileWorkload(ctx, &DeploymentWorkload{Deployment: dep})
}

func (r *KarpenterSurgeReconciler) reconcileWorkload(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	ctx = ctxWithWorkload(ctx, wl)
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	annotations := meta.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}

	currentState := DrainState(annotations[AnnotationKarpenterSurgeState])
	isActive := currentState != DrainStateNone && currentState != DrainStateDone

	if isActive {
		start, ok := parseKarpenterSurgeStart(annotations)
		if !ok {
			logger.Info("karpenter-surge state present without valid start annotation, force aborting", LogFieldKarpenterSurgeState, currentState)
			r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeAborted", "Karpenter-surge state %q present without parseable start annotation", currentState)
			return r.abortKarpenterSurge(ctx, wl)
		}
		elapsed := r.timeNow().Sub(start)
		if elapsed > 3*r.Config.KarpenterSurgeTimeout {
			logger.Info("stale karpenter-surge state detected, force aborting", LogFieldKarpenterSurgeState, currentState)
			r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeStale", "Force aborting stale karpenter-surge operation")
			return r.abortKarpenterSurge(ctx, wl)
		}
		if elapsed > r.Config.KarpenterSurgeTimeout {
			logger.Info("karpenter-surge operation timed out", LogFieldKarpenterSurgeState, currentState)
			r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeAborted", "Karpenter-surge operation timed out")
			return r.abortKarpenterSurge(ctx, wl)
		}
	}

	// Drain is exclusive: if a drain operation owns this workload, yield.
	if annotations[AnnotationDrainState] != "" {
		if isActive {
			logger.Info("drain operation took over, yielding karpenter-surge")
			return r.yieldToDrain(ctx, wl, "drain operation took precedence")
		}
		return ctrl.Result{}, nil
	}
	// Restart-surge is also exclusive (R8 inverse — if restart-surge owns it,
	// step aside).
	if annotations[AnnotationRestartSurgeState] != "" && !isActive {
		return ctrl.Result{}, nil
	}

	switch currentState {
	case DrainStateNone:
		return r.handleKarpenterPending(ctx, wl)
	case DrainStatePending:
		return r.handleKarpenterScaleUp(ctx, wl)
	case DrainStateScaledUp:
		return r.handleKarpenterWaitReady(ctx, wl)
	case DrainStateReady:
		return r.handleKarpenterReady(ctx, wl)
	case DrainStateDraining:
		return r.handleKarpenterScaleDown(ctx, wl)
	case DrainStateDone:
		return r.handleKarpenterCleanup(ctx, wl)
	default:
		logger.Info("unknown karpenter-surge state, aborting", LogFieldKarpenterSurgeState, currentState)
		return r.abortKarpenterSurge(ctx, wl)
	}
}

// handleKarpenterPending runs the gate suite documented in the spec. On
// success it stamps annotations and transitions to scaled-up via the same
// scale-up path used by drain-surge and restart-surge.
func (r *KarpenterSurgeReconciler) handleKarpenterPending(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	// Gate 2: opt-in.
	if meta.Annotations[r.Config.EnabledAnnotation] != "true" {
		return ctrl.Result{}, nil
	}
	// Gate 4: stable.
	if !wl.IsStable() {
		logger.V(1).Info("workload not stable, skipping karpenter-surge")
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}
	// Gate 5: single-replica.
	if wl.GetReplicas() != 1 {
		return ctrl.Result{}, nil
	}
	// Gate (CanSurge): Deployments with Recreate strategy cannot.
	if !wl.CanSurge() {
		logger.V(1).Info("workload does not support surge, skipping karpenter-surge")
		r.Recorder.Event(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeSkipped", "Workload strategy does not support surge")
		return ctrl.Result{}, nil
	}

	// Gate 6+7+8: PDB present, blocking, and surge would resolve it.
	pdb, err := findPDBForWorkload(ctx, r.Client, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}
	if pdb == nil {
		logger.V(1).Info("no matching PDB, skipping karpenter-surge")
		r.Recorder.Event(wl.Object(), corev1.EventTypeWarning, "NoPDB", "No PDB matches the workload's pod selector; karpenter-surge cannot act without one")
		return ctrl.Result{}, nil
	}
	blocked := isPDBBlocked(pdb)
	elapsed := r.tracker().Observe(pdb.UID, blocked, r.timeNow(), r.Config.KarpenterSurgeGracePeriod)
	if !blocked {
		return ctrl.Result{}, nil
	}
	if !pdbWouldAllowSurge(pdb, wl.GetReplicas()+1) {
		logger.Info("PDB would not allow surge, refusing to act", LogFieldKarpenterPDB, pdb.Namespace+"/"+pdb.Name)
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeSkipped", "PDB %s/%s would not be satisfied by a surge (PDBOverConstrained)", pdb.Namespace, pdb.Name)
		return ctrl.Result{}, nil
	}

	// Gate 10: workload's pod node must NOT have a drain taint already.
	hasTaint, err := r.workloadHasNodeWithDrainTaint(ctx, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}
	if hasTaint {
		logger.V(1).Info("workload's node already has a drain taint, deferring to NodeReconciler")
		return ctrl.Result{}, nil
	}

	// Gate 11: grace period vencido.
	if !elapsed {
		logger.V(1).Info("grace period not yet elapsed for blocked PDB", LogFieldKarpenterPDB, pdb.Namespace+"/"+pdb.Name, LogFieldGracePeriod, r.Config.KarpenterSurgeGracePeriod)
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	originalReplicas := wl.GetReplicas()
	if meta.Annotations == nil {
		meta.Annotations = make(map[string]string)
	}
	meta.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateScaledUp)
	meta.Annotations[AnnotationOriginalReplicas] = strconv.Itoa(int(originalReplicas))
	meta.Annotations[AnnotationKarpenterSurgeStart] = r.timeNow().UTC().Format(time.RFC3339)
	meta.Annotations[AnnotationKarpenterSurgePDB] = pdb.Namespace + "/" + pdb.Name

	// Gate 9: HPA must allow surge.
	hpa, err := FindMatchingHPA(ctx, r.Client, meta.Namespace, meta.Name, wl.GetObjectKind())
	if err != nil {
		return ctrl.Result{}, err
	}
	if hpa != nil {
		if hpa.Spec.MaxReplicas < originalReplicas+1 {
			logger.Info("HPA maxReplicas too low for karpenter-surge, skipping", LogFieldMaxReplicas, hpa.Spec.MaxReplicas)
			r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeSkipped", "HPA maxReplicas=%d prevents karpenter-surge (HPAMaxReplicasInsufficient)", hpa.Spec.MaxReplicas)
			return ctrl.Result{}, nil
		}
		if IsHPAAtMinReplicas(hpa, originalReplicas+1) {
			logger.V(1).Info("HPA already at surge target, deferring to next reconcile", LogFieldHPA, hpa.Name, LogFieldMinReplicas, *hpa.Spec.MinReplicas)
			return ctrl.Result{RequeueAfter: 250 * time.Millisecond}, nil
		}
		originalMin := HPAMinReplicasOrDefault(hpa)
		meta.Annotations[AnnotationHPAName] = hpa.Name
		meta.Annotations[AnnotationHPAOriginalMinReplicas] = strconv.Itoa(int(originalMin))
		if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch workload for karpenter-surge scale-up: %w", err)
		}
		if err := PatchHPAMinReplicas(ctx, r.Client, meta.Namespace, hpa.Name, originalReplicas+1); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch HPA minReplicas for karpenter-surge: %w", err)
		}
		logger.Info("patched HPA minReplicas for karpenter-surge", LogFieldHPA, hpa.Name, LogFieldFrom, originalMin, LogFieldTo, originalReplicas+1)
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "KarpenterSurge", "Patched HPA %s minReplicas from %d to %d to unblock Karpenter consolidation on PDB %s/%s", hpa.Name, originalMin, originalReplicas+1, pdb.Namespace, pdb.Name)
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	wl.SetReplicas(originalReplicas + 1)
	logger.Info("scaled up workload for karpenter-surge", LogFieldFrom, originalReplicas, LogFieldTo, originalReplicas+1, LogFieldKarpenterPDB, pdb.Namespace+"/"+pdb.Name)
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "KarpenterSurge", "Karpenter consolidation blocked by PDB %s/%s, scaled from %d to %d", pdb.Namespace, pdb.Name, originalReplicas, originalReplicas+1)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload for karpenter-surge scale-up: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleKarpenterScaleUp re-applies the surge if a competing controller
// reset spec.replicas (no-HPA path) or if the HPA's minReplicas drifted
// from the expected original+1 (typically because handleKarpenterPending's
// HPA patch transiently failed after the workload annotations were already
// persisted — without this re-apply, the workload would sit in scaled-up
// until KarpenterSurgeTimeout).
func (r *KarpenterSurgeReconciler) handleKarpenterScaleUp(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		logger.Error(err, "cannot determine original replicas, aborting karpenter-surge")
		return r.abortKarpenterSurge(ctx, wl)
	}

	if hpaName := meta.Annotations[AnnotationHPAName]; hpaName != "" {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: meta.Namespace, Name: hpaName}, hpa); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("HPA disappeared mid-surge, aborting karpenter-surge")
				return r.abortKarpenterSurge(ctx, wl)
			}
			return ctrl.Result{}, fmt.Errorf("get HPA for scale-up reconcile: %w", err)
		}
		current := HPAMinReplicasOrDefault(hpa)
		if current != original+1 {
			logger.Info("HPA minReplicas drifted from expected surge target, re-applying", LogFieldHPA, hpaName, LogFieldFrom, current, LogFieldTo, original+1)
			if err := PatchHPAMinReplicas(ctx, r.Client, meta.Namespace, hpaName, original+1); err != nil {
				return ctrl.Result{}, fmt.Errorf("re-apply HPA minReplicas: %w", err)
			}
		}
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	if wl.GetReplicas() > original {
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	logger.Info("replicas were reset, competing controller detected — re-applying karpenter-surge")
	r.Recorder.Event(wl.Object(), corev1.EventTypeWarning, "CompetingController", "Replicas were reset externally during karpenter-surge")
	wl.SetReplicas(original + 1)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-apply karpenter-surge scale-up: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleKarpenterWaitReady waits until at least 2 selector-matched,
// non-terminating pods are Ready.
func (r *KarpenterSurgeReconciler) handleKarpenterWaitReady(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if meta.Annotations[AnnotationHPAName] == "" {
		original, err := getOriginalReplicas(meta.Annotations)
		if err != nil {
			logger.Error(err, "cannot determine original replicas, aborting karpenter-surge")
			return r.abortKarpenterSurge(ctx, wl)
		}
		if wl.GetReplicas() <= original {
			logger.Info("replicas were reset externally during wait-ready, re-applying karpenter-surge")
			wl.SetReplicas(original + 1)
			if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
				return ctrl.Result{}, fmt.Errorf("re-apply karpenter-surge scale-up: %w", err)
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
		logger.Info("surge replica ready, Karpenter may proceed", LogFieldReplicas, readyCount)
		r.Recorder.Event(wl.Object(), corev1.EventTypeNormal, "KarpenterSurgeReady", "Surge replica ready; Karpenter may now proceed with consolidation")
		meta.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateReady)
		if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch workload to ready: %w", err)
		}
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}
	logger.V(1).Info("waiting for surge replica to become ready", LogFieldReplicas, readyCount)
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleKarpenterReady waits for one of three transitions: taint appears
// (yield), the original pod is gone (proceed to scale-down), or timeout.
func (r *KarpenterSurgeReconciler) handleKarpenterReady(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	// Taint on any node hosting a pod of this workload → yield.
	taint, err := r.workloadHasNodeWithDrainTaint(ctx, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}
	if taint {
		logger.Info("drain taint detected on workload node, yielding to NodeReconciler")
		return r.yieldToDrain(ctx, wl, "drain taint appeared on workload node")
	}

	// PDB unblocked AND pod count back at original+1 baseline?
	pdbName := meta.Annotations[AnnotationKarpenterSurgePDB]
	if pdbName != "" {
		pdbNS, pdbN := splitNamespacedName(pdbName)
		var pdb policyv1.PodDisruptionBudget
		err := r.Get(ctx, types.NamespacedName{Namespace: pdbNS, Name: pdbN}, &pdb)
		if err == nil && !isPDBBlocked(&pdb) {
			// PDB recovered — but did our pod predecessor go away, or did the
			// operator just unblock things by scaling up? Distinguish via
			// remaining pod count: if there are still > original+1 Running
			// pods that pre-date the surge start, R9 will take over in
			// scale-down. Either way, transition to draining and let R9
			// adjudicate.
			logger.Info("PDB unblocked, transitioning to draining", LogFieldKarpenterPDB, pdbName)
			meta.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateDraining)
			if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
				return ctrl.Result{}, fmt.Errorf("patch workload to draining: %w", err)
			}
			return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
		}
	}
	logger.V(1).Info("waiting for Karpenter to proceed (taint or pod eviction)")
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleKarpenterScaleDown enforces the R9 invariant: only undo what we
// applied. If the operator/ArgoCD moved replicas to a different target,
// yield with ExternalScaleChange instead of overwriting their decision.
func (r *KarpenterSurgeReconciler) handleKarpenterScaleDown(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		logger.Error(err, "cannot determine original replicas, aborting karpenter-surge")
		return r.abortKarpenterSurge(ctx, wl)
	}
	hpaName := meta.Annotations[AnnotationHPAName]

	if hpaName == "" {
		// R9 invariant: spec.replicas must be exactly what we set (original+1).
		if wl.GetReplicas() != original+1 {
			logger.Info("replicas changed externally, yielding without scale-back", LogFieldReplicas, wl.GetReplicas(), LogFieldFrom, original+1)
			return r.yieldKarpenterSurge(ctx, wl, "ExternalScaleChange")
		}
		// Restore HPA is a no-op here; we only need to scale replicas back.
		wl.SetReplicas(original)
	} else {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: meta.Namespace, Name: hpaName}, hpa); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("HPA disappeared mid-cycle, aborting karpenter-surge")
				return r.abortKarpenterSurge(ctx, wl)
			}
			return ctrl.Result{}, fmt.Errorf("get HPA for scale-down: %w", err)
		}
		current := HPAMinReplicasOrDefault(hpa)
		if current != original+1 {
			logger.Info("HPA minReplicas changed externally, yielding without restore", LogFieldMinReplicas, current, LogFieldFrom, original+1)
			return r.yieldKarpenterSurge(ctx, wl, "ExternalScaleChange")
		}
		if err := r.restoreHPA(ctx, meta); err != nil {
			return ctrl.Result{}, fmt.Errorf("restore HPA after karpenter-surge: %w", err)
		}
	}

	meta.Annotations[AnnotationKarpenterSurgeState] = string(DrainStateDone)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload for karpenter-surge scale-down: %w", err)
	}
	logger.Info("karpenter-surge scale-down complete")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "KarpenterSurgeScaleDown", "Scaled down to %d after Karpenter consolidation completed", original)
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *KarpenterSurgeReconciler) handleKarpenterCleanup(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	clearKarpenterSurgeAnnotations(meta.Annotations)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload for karpenter-surge cleanup: %w", err)
	}
	logger.Info("karpenter-surge operation completed")
	r.Recorder.Event(wl.Object(), corev1.EventTypeNormal, "KarpenterSurgeComplete", "Karpenter-surge operation completed successfully")
	return ctrl.Result{}, nil
}

func (r *KarpenterSurgeReconciler) abortKarpenterSurge(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if err := r.restoreHPA(ctx, meta); err != nil {
		logger.Error(err, "failed to restore HPA during karpenter-surge abort")
	}
	if original, err := getOriginalReplicas(meta.Annotations); err == nil && meta.Annotations[AnnotationHPAName] == "" {
		wl.SetReplicas(original)
	}
	clearKarpenterSurgeAnnotations(meta.Annotations)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("abort karpenter-surge: %w", err)
	}
	logger.Info("aborted karpenter-surge operation")
	r.Recorder.Event(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeAborted", "Karpenter-surge operation aborted")
	return ctrl.Result{}, nil
}

// yieldToDrain restores replicas to original and clears only the exclusive
// karpenter-surge annotations so the drain machinery picks up shared keys.
func (r *KarpenterSurgeReconciler) yieldToDrain(ctx context.Context, wl DrainableWorkload, reason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if meta.Annotations[AnnotationHPAName] == "" {
		if original, err := getOriginalReplicas(meta.Annotations); err == nil {
			wl.SetReplicas(original)
		}
	}
	clearKarpenterSurgeExclusiveAnnotations(meta.Annotations)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("yield karpenter-surge to drain: %w", err)
	}
	logger.Info("yielded karpenter-surge to drain", LogFieldReason, reason)
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeYielded", "Karpenter-surge yielded: %s", reason)
	return ctrl.Result{}, nil
}

// yieldKarpenterSurge handles the R9 path: external replica change.
// Differs from yieldToDrain in that we do NOT touch replicas (the operator
// just changed them); we only clear our annotations.
func (r *KarpenterSurgeReconciler) yieldKarpenterSurge(ctx context.Context, wl DrainableWorkload, reason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	clearKarpenterSurgeAnnotations(meta.Annotations)
	if err := wl.PatchOwned(ctx, r.Client, karpenterSurgeOwnedKeys); err != nil {
		return ctrl.Result{}, fmt.Errorf("yield karpenter-surge: %w", err)
	}
	logger.Info("yielded karpenter-surge", LogFieldReason, reason)
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "KarpenterSurgeYielded", "Karpenter-surge yielded: %s", reason)
	return ctrl.Result{}, nil
}

func (r *KarpenterSurgeReconciler) restoreHPA(ctx context.Context, meta *metav1.ObjectMeta) error {
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

func (r *KarpenterSurgeReconciler) workloadHasNodeWithDrainTaint(ctx context.Context, namespace string, sel labels.Selector) (bool, error) {
	pods, err := listMatchingPods(ctx, r.Client, namespace, sel)
	if err != nil {
		return false, err
	}
	for i := range pods {
		nodeName := pods[i].Spec.NodeName
		if nodeName == "" {
			continue
		}
		var node corev1.Node
		if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, err
		}
		for _, t := range node.Spec.Taints {
			for _, dt := range r.Config.DrainTaints {
				if t.Key == dt.Key {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// RecoverOrphans aborts karpenter-surge operations whose PDB no longer
// exists or has recovered. Called once on leader election.
func (r *KarpenterSurgeReconciler) RecoverOrphans(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("karpenter-surge-orphan-recovery")
	logger.Info("starting karpenter-surge orphan recovery scan")

	if r.RolloutsAvailable {
		var roList rolloutsv1alpha1.RolloutList
		if err := r.List(ctx, &roList); err != nil {
			return fmt.Errorf("list rollouts: %w", err)
		}
		for i := range roList.Items {
			ro := &roList.Items[i]
			if ro.Annotations[AnnotationKarpenterSurgeState] == "" {
				continue
			}
			abort, err := r.shouldAbortOrphan(ctx, &ro.ObjectMeta)
			if err != nil {
				logger.Error(err, "transient error checking rollout orphan, skipping for this scan", LogFieldWorkload, ro.Name, LogFieldNamespace, ro.Namespace)
				continue
			}
			if abort {
				logger.Info("found orphaned karpenter-surge rollout", LogFieldWorkload, ro.Name, LogFieldNamespace, ro.Namespace)
				if _, err := r.abortKarpenterSurge(ctx, &RolloutWorkload{Rollout: ro}); err != nil {
					logger.Error(err, "failed to abort orphaned karpenter-surge rollout", LogFieldWorkload, ro.Name)
				}
			}
		}
	}

	var depList appsv1.DeploymentList
	if err := r.List(ctx, &depList); err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}
	for i := range depList.Items {
		dep := &depList.Items[i]
		if dep.Annotations[AnnotationKarpenterSurgeState] == "" {
			continue
		}
		abort, err := r.shouldAbortOrphan(ctx, &dep.ObjectMeta)
		if err != nil {
			logger.Error(err, "transient error checking deployment orphan, skipping for this scan", LogFieldWorkload, dep.Name, LogFieldNamespace, dep.Namespace)
			continue
		}
		if abort {
			logger.Info("found orphaned karpenter-surge deployment", LogFieldWorkload, dep.Name, LogFieldNamespace, dep.Namespace)
			if _, err := r.abortKarpenterSurge(ctx, &DeploymentWorkload{Deployment: dep}); err != nil {
				logger.Error(err, "failed to abort orphaned karpenter-surge deployment", LogFieldWorkload, dep.Name)
			}
		}
	}

	logger.Info("karpenter-surge orphan recovery scan complete")
	return nil
}

// shouldAbortOrphan decides whether to abort a workload's karpenter-surge
// during leader-election orphan recovery. Returns (abort, err). A non-nil
// err indicates a transient apiserver failure: the caller should log and
// skip the workload for this scan (the normal reconcile loop will revisit
// it when the next event fires); without this signal we would silently
// leave orphans un-aborted across the entire leader term.
func (r *KarpenterSurgeReconciler) shouldAbortOrphan(ctx context.Context, meta *metav1.ObjectMeta) (bool, error) {
	if !r.Config.KarpenterSurgeEnabled {
		return true, nil
	}
	if start, ok := parseKarpenterSurgeStart(meta.Annotations); ok {
		if r.timeNow().Sub(start) > 3*r.Config.KarpenterSurgeTimeout {
			return true, nil
		}
	}
	pdbName := meta.Annotations[AnnotationKarpenterSurgePDB]
	if pdbName == "" {
		return true, nil
	}
	pdbNS, pdbN := splitNamespacedName(pdbName)
	var pdb policyv1.PodDisruptionBudget
	if err := r.Get(ctx, types.NamespacedName{Namespace: pdbNS, Name: pdbN}, &pdb); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return !isPDBBlocked(&pdb), nil
}

func (r *KarpenterSurgeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueChan := make(chan event.GenericEvent, 16)
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		return r.runBackupScanner(ctx, enqueueChan)
	})); err != nil {
		return fmt.Errorf("add karpenter-surge backup scanner: %w", err)
	}

	// Deployment is the unconditional primary (works on any cluster); the
	// Rollout watch only attaches when the Argo Rollouts CRD is present.
	b := ctrl.NewControllerManagedBy(mgr).
		Named("karpenter-surge").
		For(&appsv1.Deployment{}, builder.WithPredicates(karpenterWorkloadPredicate(r.Config.EnabledAnnotation))).
		Watches(&policyv1.PodDisruptionBudget{}, handler.EnqueueRequestsFromMapFunc(r.mapPDBToWorkload), builder.WithPredicates(karpenterPDBPredicate())).
		WatchesRawSource(source.Channel(enqueueChan, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: o.GetName()}}}
		}))).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5})

	if r.RolloutsAvailable {
		b = b.Watches(&rolloutsv1alpha1.Rollout{}, handler.EnqueueRequestsFromMapFunc(r.mapWorkloadToSelf), builder.WithPredicates(karpenterWorkloadPredicate(r.Config.EnabledAnnotation)))
	}
	return b.Complete(r)
}

// runBackupScanner ticks every cfg.KarpenterSurgeScanPeriod and emits one
// synthetic event per workload whose PDB is currently stuck. Covers the
// case where the informer for PDBs drifts out of sync.
func (r *KarpenterSurgeReconciler) runBackupScanner(ctx context.Context, out chan<- event.GenericEvent) error {
	logger := log.FromContext(ctx).WithName("karpenter-surge-scanner")
	logger.Info("starting karpenter-surge backup scanner", LogFieldGracePeriod, r.Config.KarpenterSurgeScanPeriod)
	ticker := time.NewTicker(r.Config.KarpenterSurgeScanPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.scanOnce(ctx, out)
		}
	}
}

func (r *KarpenterSurgeReconciler) scanOnce(ctx context.Context, out chan<- event.GenericEvent) {
	logger := log.FromContext(ctx).WithName("karpenter-surge-scanner")
	var pdbList policyv1.PodDisruptionBudgetList
	if err := r.List(ctx, &pdbList); err != nil {
		logger.Error(err, "failed to list PDBs")
		return
	}
	for i := range pdbList.Items {
		pdb := &pdbList.Items[i]
		if !isPDBBlocked(pdb) {
			r.tracker().Observe(pdb.UID, false, r.timeNow(), r.Config.KarpenterSurgeGracePeriod)
			continue
		}
		for _, req := range r.mapPDBToWorkload(ctx, pdb) {
			ev := event.GenericEvent{Object: &metav1.PartialObjectMetadata{
				ObjectMeta: metav1.ObjectMeta{Namespace: req.Namespace, Name: req.Name},
			}}
			// Non-blocking send with ctx cancellation. Under cluster-wide
			// cascade the buffer (cap 16) can fill before reconcilers drain
			// it; rather than block the scanner (and leak this goroutine on
			// leader loss), drop the synthetic event — the next tick will
			// retry, and the PDB watch is still the primary trigger.
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			default:
				logger.V(1).Info("scanner enqueue channel full, dropping event", LogFieldKarpenterPDB, pdb.Namespace+"/"+pdb.Name)
			}
		}
	}
}

func (r *KarpenterSurgeReconciler) mapWorkloadToSelf(_ context.Context, obj client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}}}
}


// karpenterWorkloadPredicate filters Rollout/Deployment events: keep
// reconciling anything already mid-operation, plus anything opted-in (the
// PDB watch handles the rest).
func karpenterWorkloadPredicate(enabledAnnotation string) predicate.Predicate {
	relevant := func(o client.Object) bool {
		ann := o.GetAnnotations()
		if ann[AnnotationKarpenterSurgeState] != "" {
			return true
		}
		return ann[enabledAnnotation] == "true"
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return relevant(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return relevant(e.ObjectNew) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return relevant(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return relevant(e.Object) },
	}
}

// findPDBForWorkload returns the PDB whose selector subset-matches the
// workload pod selector, or nil. Mirrors the semantics of FindMatchingPDB
// but returns the PDB itself (we need its status).
func findPDBForWorkload(ctx context.Context, c client.Client, namespace string, sel labels.Selector) (*policyv1.PodDisruptionBudget, error) {
	var pdbList policyv1.PodDisruptionBudgetList
	if err := c.List(ctx, &pdbList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list PDBs: %w", err)
	}
	workloadReqs, _ := sel.Requirements()
	for i := range pdbList.Items {
		pdb := &pdbList.Items[i]
		if pdb.Spec.Selector == nil {
			continue
		}
		pdbSel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		pdbReqs, _ := pdbSel.Requirements()
		if requirementsMatch(pdbReqs, workloadReqs) {
			return pdb, nil
		}
	}
	return nil, nil
}

func splitNamespacedName(s string) (namespace, name string) {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return s[:i], s[i+1:]
		}
	}
	return "", s
}
