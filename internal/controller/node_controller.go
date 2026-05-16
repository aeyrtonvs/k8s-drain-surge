package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aeyrtonvs/k8s-drain-surge/internal/config"
)

type NodeReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Config   *config.Config
}

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues(LogFieldNode, req.Name)
	ctx = log.IntoContext(ctx, logger)

	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("node not found, aborting associated workloads")
			return r.abortWorkloadsForNode(ctx, req.Name)
		}
		return ctrl.Result{}, err
	}

	if !r.hasDrainTaint(&node) {
		return r.abortWorkloadsForNode(ctx, node.Name)
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{IndexPodNodeName: node.Name}); err != nil {
		return ctrl.Result{}, fmt.Errorf("list pods on node: %w", err)
	}

	// Resolve workloads from pods, deduplicating by namespace/name.
	workloads := make(map[string]DrainableWorkload)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed || pod.DeletionTimestamp != nil {
			continue
		}

		wl, err := ResolveWorkloadFromPod(ctx, r.Client, pod)
		if err != nil {
			logger.Error(err, "failed to resolve workload", LogFieldPod, pod.Name)
			continue
		}
		if wl == nil {
			continue
		}

		meta := wl.GetObjectMeta()
		key := workloadKey(meta.Namespace, meta.Name)
		if _, exists := workloads[key]; !exists {
			workloads[key] = wl
		}
	}

	// Include workloads whose pods may have already been evicted but still
	// have a drain-node annotation pointing to this node.
	existingWorkloads, err := r.findWorkloadsWithDrainNode(ctx, node.Name)
	if err != nil {
		logger.Error(err, "failed to find existing drain workloads")
	}
	for key, wl := range existingWorkloads {
		if _, exists := workloads[key]; !exists {
			workloads[key] = wl
		}
	}

	if len(workloads) == 0 {
		return ctrl.Result{}, nil
	}

	var requeueAfter time.Duration
	for key, wl := range workloads {
		result, err := r.reconcileWorkload(ctx, wl, node.Name)
		if err != nil {
			logger.Error(err, "failed to reconcile workload", LogFieldWorkload, key)
			continue
		}
		if result.RequeueAfter > 0 && (requeueAfter == 0 || result.RequeueAfter < requeueAfter) {
			requeueAfter = result.RequeueAfter
		}
	}

	if requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) reconcileWorkload(ctx context.Context, wl DrainableWorkload, nodeName string) (ctrl.Result, error) {
	ctx = ctxWithWorkload(ctx, wl)
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	annotations := meta.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}

	currentState := DrainState(annotations[AnnotationDrainState])
	isActive := currentState != DrainStateNone && currentState != DrainStateDone

	// Parse drain start timestamp once for both stale and timeout checks.
	if isActive {
		if drainStart, ok := parseDrainStart(annotations); ok {
			elapsed := time.Since(drainStart)

			if elapsed > 3*r.Config.ReadinessTimeout {
				logger.Info("stale drain state detected, force aborting", LogFieldState, currentState)
				r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainStale", "Force aborting stale drain operation on node %s", nodeName)
				return r.abortWorkload(ctx, wl)
			}

			if elapsed > r.Config.ReadinessTimeout {
				logger.Info("drain operation timed out", LogFieldState, currentState)
				r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainTimeout", "Drain operation timed out on node %s", nodeName)
				return r.abortWorkload(ctx, wl)
			}
		}
	}

	if currentState == DrainStateNone || currentState == "" {
		if !r.shouldProcess(ctx, wl, nodeName) {
			return ctrl.Result{}, nil
		}
	} else {
		if drainNode := annotations[AnnotationDrainNode]; drainNode != "" && drainNode != nodeName {
			logger.V(1).Info("workload is being drained by another node", LogFieldOtherNode, drainNode)
			return ctrl.Result{}, nil
		}
	}

	switch currentState {
	case DrainStateNone:
		return r.handlePending(ctx, wl, nodeName)
	case DrainStatePending:
		return r.handleScaleUp(ctx, wl, nodeName)
	case DrainStateScaledUp:
		return r.handleWaitReady(ctx, wl, nodeName)
	case DrainStateReady:
		return r.handleWaitEviction(ctx, wl, nodeName)
	case DrainStateDraining:
		return r.handleScaleDown(ctx, wl)
	case DrainStateDone:
		return r.handleCleanup(ctx, wl)
	default:
		logger.Info("unknown drain state, aborting", LogFieldState, currentState)
		return r.abortWorkload(ctx, wl)
	}
}

func (r *NodeReconciler) handlePending(ctx context.Context, wl DrainableWorkload, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	hasPDB, err := FindMatchingPDB(ctx, r.Client, meta.Namespace, wl.GetPodSelector())
	if err != nil {
		return ctrl.Result{}, err
	}
	if !hasPDB {
		logger.V(1).Info("no matching PDB found, skipping workload")
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "NoPDB", "No PodDisruptionBudget found matching workload pods — skipping drain surge")
		return ctrl.Result{}, nil
	}

	originalReplicas := wl.GetReplicas()
	if meta.Annotations == nil {
		meta.Annotations = make(map[string]string)
	}
	meta.Annotations[AnnotationDrainState] = string(DrainStateScaledUp)
	meta.Annotations[AnnotationOriginalReplicas] = strconv.Itoa(int(originalReplicas))
	meta.Annotations[AnnotationDrainNode] = nodeName
	meta.Annotations[AnnotationDrainStart] = time.Now().UTC().Format(time.RFC3339)

	// Check if an HPA manages this workload. If so, patch HPA minReplicas
	// instead of the workload's spec.replicas — the HPA always wins.
	hpa, err := FindMatchingHPA(ctx, r.Client, meta.Namespace, meta.Name, wl.GetObjectKind())
	if err != nil {
		return ctrl.Result{}, err
	}

	if hpa != nil {
		if hpa.Spec.MaxReplicas < originalReplicas+1 {
			logger.Info("HPA maxReplicas too low for surge, skipping", LogFieldMaxReplicas, hpa.Spec.MaxReplicas)
			r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainSkipped", "HPA maxReplicas=%d prevents drain surge", hpa.Spec.MaxReplicas)
			return ctrl.Result{}, nil
		}

		originalMin := int32(1)
		if hpa.Spec.MinReplicas != nil {
			originalMin = *hpa.Spec.MinReplicas
		}
		meta.Annotations[AnnotationHPAName] = hpa.Name
		meta.Annotations[AnnotationHPAOriginalMinReplicas] = strconv.Itoa(int(originalMin))

		if err := PatchHPAMinReplicas(ctx, r.Client, meta.Namespace, hpa.Name, originalReplicas+1); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch HPA minReplicas for scale-up: %w", err)
		}
		logger.Info("patched HPA minReplicas for surge", LogFieldHPA, hpa.Name, LogFieldFrom, originalMin, LogFieldTo, originalReplicas+1)
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "DrainSurge", "Patched HPA %s minReplicas from %d to %d for node drain on %s", hpa.Name, originalMin, originalReplicas+1, nodeName)
	} else {
		wl.SetReplicas(originalReplicas + 1)
		logger.Info("scaled up workload", LogFieldFrom, originalReplicas, LogFieldTo, originalReplicas+1)
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "DrainSurge", "Scaled up from %d to %d for node drain on %s", originalReplicas, originalReplicas+1, nodeName)
	}

	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload for scale-up: %w", err)
	}

	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

// handleScaleUp is a reentrant safety net: if replicas were reset externally
// (e.g. by ArgoCD), re-apply the scale-up.
func (r *NodeReconciler) handleScaleUp(ctx context.Context, wl DrainableWorkload, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		logger.Error(err, "cannot determine original replicas, aborting")
		return r.abortWorkload(ctx, wl)
	}

	if wl.GetReplicas() <= original {
		logger.Info("replicas were reset, competing controller detected — re-applying scale-up")
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "CompetingController", "Replicas were reset externally during drain surge")
		wl.SetReplicas(original + 1)
	}

	meta.Annotations[AnnotationDrainState] = string(DrainStateScaledUp)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload for scale-up (reentrant): %w", err)
	}

	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *NodeReconciler) handleWaitReady(ctx context.Context, wl DrainableWorkload, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	// For non-HPA workloads, re-apply scale-up if replicas were reset externally (e.g. by ArgoCD).
	if meta.Annotations[AnnotationHPAName] == "" {
		original, err := getOriginalReplicas(meta.Annotations)
		if err != nil {
			logger.Error(err, "cannot determine original replicas, aborting")
			return r.abortWorkload(ctx, wl)
		}
		if wl.GetReplicas() <= original {
			logger.Info("replicas were reset externally, re-applying scale-up")
			wl.SetReplicas(original + 1)
			if err := wl.Patch(ctx, r.Client); err != nil {
				return ctrl.Result{}, fmt.Errorf("re-apply scale-up: %w", err)
			}
			return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
		}
	}

	ready, err := FindReadyPodOnOtherNode(ctx, r.Client, meta.Namespace, wl.GetPodSelector(), nodeName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if ready {
		logger.Info("new pod is ready on another node")
		meta.Annotations[AnnotationDrainState] = string(DrainStateReady)
		if err := wl.Patch(ctx, r.Client); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch workload to ready: %w", err)
		}
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	logger.V(1).Info("waiting for new pod to become ready on another node")
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *NodeReconciler) handleWaitEviction(ctx context.Context, wl DrainableWorkload, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	podOnNode, err := FindPodOnNode(ctx, r.Client, meta.Namespace, wl.GetPodSelector(), nodeName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !podOnNode {
		// Restore HPA minReplicas if we patched it.
		if err := r.restoreHPA(ctx, meta); err != nil {
			return ctrl.Result{}, fmt.Errorf("restore HPA after eviction: %w", err)
		}

		original, err := getOriginalReplicas(meta.Annotations)
		if err != nil {
			logger.Error(err, "cannot determine original replicas, aborting")
			return r.abortWorkload(ctx, wl)
		}

		// For non-HPA workloads, scale down directly.
		if meta.Annotations[AnnotationHPAName] == "" {
			wl.SetReplicas(original)
		}
		meta.Annotations[AnnotationDrainState] = string(DrainStateDraining)
		if err := wl.Patch(ctx, r.Client); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch workload for scale-down: %w", err)
		}
		logger.Info("old pod evicted, scaling down")
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "DrainScaleDown", "Scaled down to %d after pod eviction from %s", original, nodeName)
		return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
	}

	logger.V(1).Info("waiting for old pod to be evicted")
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *NodeReconciler) handleScaleDown(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	meta := wl.GetObjectMeta()

	original, err := getOriginalReplicas(meta.Annotations)
	if err != nil {
		return r.abortWorkload(ctx, wl)
	}

	// For non-HPA workloads, scale down directly.
	if meta.Annotations[AnnotationHPAName] == "" && wl.GetReplicas() != original {
		wl.SetReplicas(original)
	}
	meta.Annotations[AnnotationDrainState] = string(DrainStateDone)
	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload to done: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Config.RequeueInterval}, nil
}

func (r *NodeReconciler) handleCleanup(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	clearDrainAnnotations(meta.Annotations)

	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch workload for cleanup: %w", err)
	}
	logger.Info("drain operation completed")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeNormal, "DrainComplete", "Drain surge operation completed successfully")
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) abortWorkload(ctx context.Context, wl DrainableWorkload) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	// Restore HPA minReplicas if we patched it.
	if err := r.restoreHPA(ctx, meta); err != nil {
		logger.Error(err, "failed to restore HPA during abort")
	}

	if original, err := getOriginalReplicas(meta.Annotations); err == nil {
		wl.SetReplicas(original)
	}

	clearDrainAnnotations(meta.Annotations)

	if err := wl.Patch(ctx, r.Client); err != nil {
		return ctrl.Result{}, fmt.Errorf("abort workload: %w", err)
	}
	logger.Info("aborted drain operation")
	r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainAborted", "Drain surge operation aborted")
	return ctrl.Result{}, nil
}

// abortWorkloadsForNode aborts all workloads with drain-node annotation pointing
// to the given node. Accumulates errors so all workloads are attempted.
func (r *NodeReconciler) abortWorkloadsForNode(ctx context.Context, nodeName string) (ctrl.Result, error) {
	workloads, err := r.findWorkloadsWithDrainNode(ctx, nodeName)
	if err != nil {
		return ctrl.Result{}, err
	}
	var errs []error
	for _, wl := range workloads {
		if _, err := r.abortWorkload(ctx, wl); err != nil {
			errs = append(errs, err)
		}
	}
	return ctrl.Result{}, errors.Join(errs...)
}

func (r *NodeReconciler) findWorkloadsWithDrainNode(ctx context.Context, nodeName string) (map[string]DrainableWorkload, error) {
	workloads := make(map[string]DrainableWorkload)
	match := client.MatchingFields{IndexWorkloadDrainNode: nodeName}

	var rolloutList rolloutsv1alpha1.RolloutList
	if err := r.List(ctx, &rolloutList, match); err != nil {
		return nil, fmt.Errorf("list rollouts: %w", err)
	}
	for i := range rolloutList.Items {
		ro := &rolloutList.Items[i]
		workloads[workloadKey(ro.Namespace, ro.Name)] = &RolloutWorkload{Rollout: ro}
	}

	var depList appsv1.DeploymentList
	if err := r.List(ctx, &depList, match); err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	for i := range depList.Items {
		dep := &depList.Items[i]
		workloads[workloadKey(dep.Namespace, dep.Name)] = &DeploymentWorkload{Deployment: dep}
	}

	return workloads, nil
}

func (r *NodeReconciler) shouldProcess(ctx context.Context, wl DrainableWorkload, nodeName string) bool {
	logger := log.FromContext(ctx)
	meta := wl.GetObjectMeta()

	if meta.Annotations[r.Config.EnabledAnnotation] != "true" {
		return false
	}

	if wl.GetReplicas() != 1 {
		logger.V(1).Info("workload has more than 1 replica, skipping", LogFieldReplicas, wl.GetReplicas())
		return false
	}

	if !wl.IsStable() {
		logger.V(1).Info("workload is not stable, skipping")
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainSkipped", "Workload is not in a stable state — skipping drain surge")
		return false
	}

	if drainNode := meta.Annotations[AnnotationDrainNode]; drainNode != "" && drainNode != nodeName {
		logger.V(1).Info("workload is being drained by another node", LogFieldOtherNode, drainNode)
		return false
	}

	if !wl.CanSurge() {
		logger.V(1).Info("workload strategy does not support surge, skipping")
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainSkipped", "Workload strategy does not support drain surge")
		return false
	}

	hpa, err := FindMatchingHPA(ctx, r.Client, meta.Namespace, meta.Name, wl.GetObjectKind())
	if err != nil {
		logger.Error(err, "failed to check HPA")
		return false
	}
	if hpa != nil && hpa.Spec.MaxReplicas <= 1 {
		logger.V(1).Info("HPA maxReplicas is 1, cannot surge")
		r.Recorder.Eventf(wl.Object(), corev1.EventTypeWarning, "DrainSkipped", "HPA maxReplicas=1 prevents drain surge")
		return false
	}

	return true
}

// restoreHPA restores the HPA minReplicas to the original value if the controller
// patched it. This is a no-op if no HPA annotation is present.
func (r *NodeReconciler) restoreHPA(ctx context.Context, meta *metav1.ObjectMeta) error {
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

func (r *NodeReconciler) hasDrainTaint(node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		for _, dt := range r.Config.DrainTaints {
			if taint.Key == dt.Key {
				return true
			}
		}
	}
	return false
}

// RecoverOrphans scans for workloads with drain annotations that don't correspond
// to a tainted node. Called on startup after leader election.
func (r *NodeReconciler) RecoverOrphans(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("orphan-recovery")
	logger.Info("starting orphan recovery scan")

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	taintedNodes := make(map[string]bool, len(nodeList.Items))
	for i := range nodeList.Items {
		if r.hasDrainTaint(&nodeList.Items[i]) {
			taintedNodes[nodeList.Items[i].Name] = true
		}
	}

	// Collect all annotated workloads by scanning once (reusing findWorkloadsWithDrainNode
	// is not ideal here since we need workloads for ALL nodes, not a specific one).
	var rolloutList rolloutsv1alpha1.RolloutList
	if err := r.List(ctx, &rolloutList); err != nil {
		return fmt.Errorf("list rollouts: %w", err)
	}
	for i := range rolloutList.Items {
		ro := &rolloutList.Items[i]
		if drainNode := ro.Annotations[AnnotationDrainNode]; drainNode != "" && !taintedNodes[drainNode] {
			logger.Info("found orphaned rollout", LogFieldRollout, ro.Name, LogFieldNamespace, ro.Namespace, LogFieldDrainNode, drainNode)
			if _, err := r.abortWorkload(ctx, &RolloutWorkload{Rollout: ro}); err != nil {
				logger.Error(err, "failed to abort orphaned rollout", LogFieldRollout, ro.Name)
			}
		}
	}

	var depList appsv1.DeploymentList
	if err := r.List(ctx, &depList); err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}
	for i := range depList.Items {
		dep := &depList.Items[i]
		if drainNode := dep.Annotations[AnnotationDrainNode]; drainNode != "" && !taintedNodes[drainNode] {
			logger.Info("found orphaned deployment", LogFieldDeployment, dep.Name, LogFieldNamespace, dep.Namespace, LogFieldDrainNode, drainNode)
			if _, err := r.abortWorkload(ctx, &DeploymentWorkload{Deployment: dep}); err != nil {
				logger.Error(err, "failed to abort orphaned deployment", LogFieldDeployment, dep.Name)
			}
		}
	}

	logger.Info("orphan recovery scan complete")
	return nil
}

func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, IndexPodNodeName, func(o client.Object) []string {
		pod := o.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}); err != nil {
		return fmt.Errorf("create pod node index: %w", err)
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &rolloutsv1alpha1.Rollout{}, IndexWorkloadDrainNode, indexByDrainNodeAnnotation); err != nil {
		return fmt.Errorf("create rollout drain-node index: %w", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &appsv1.Deployment{}, IndexWorkloadDrainNode, indexByDrainNodeAnnotation); err != nil {
		return fmt.Errorf("create deployment drain-node index: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPodToNode)).
		Watches(&rolloutsv1alpha1.Rollout{}, handler.EnqueueRequestsFromMapFunc(r.mapWorkloadToNode)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.mapWorkloadToNode)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		Complete(r)
}

// indexByDrainNodeAnnotation returns the value of AnnotationDrainNode as the
// index key, or nil when the annotation is unset. Used by the cache field
// indexer so List(... MatchingFields{IndexWorkloadDrainNode: node}) is O(matches).
func indexByDrainNodeAnnotation(o client.Object) []string {
	v := o.GetAnnotations()[AnnotationDrainNode]
	if v == "" {
		return nil
	}
	return []string{v}
}

func (r *NodeReconciler) mapPodToNode(_ context.Context, obj client.Object) []reconcile.Request {
	pod := obj.(*corev1.Pod)
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pod.Spec.NodeName}}}
}

func (r *NodeReconciler) mapWorkloadToNode(_ context.Context, obj client.Object) []reconcile.Request {
	nodeName := obj.GetAnnotations()[AnnotationDrainNode]
	if nodeName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: nodeName}}}
}
