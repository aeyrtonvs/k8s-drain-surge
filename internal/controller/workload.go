package controller

import (
	"context"
	"encoding/json"
	"fmt"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ctxWithWorkload returns a context carrying a logger annotated with the
// canonical workload fields (name, namespace, kind).
func ctxWithWorkload(ctx context.Context, wl DrainableWorkload) context.Context {
	meta := wl.GetObjectMeta()
	logger := log.FromContext(ctx).WithValues(
		LogFieldWorkload, meta.Name,
		LogFieldNamespace, meta.Namespace,
		LogFieldKind, wl.GetObjectKind(),
	)
	return log.IntoContext(ctx, logger)
}

// DrainableWorkload abstracts the differences between Argo Rollouts and Deployments.
type DrainableWorkload interface {
	GetReplicas() int32
	SetReplicas(int32)
	IsStable() bool
	CanSurge() bool
	GetPodSelector() labels.Selector
	GetObjectMeta() *metav1.ObjectMeta
	GetObjectKind() string
	// PatchOwned applies a MergePatch with spec.replicas and annotations. Any
	// key in ownedKeys absent from the workload's in-memory annotations map
	// is set to null so the merge patch deletes it server-side. Callers must
	// pass ONLY the keys that the calling reconciler administers; passing a
	// superset would clobber peers' annotations across reconcilers.
	PatchOwned(ctx context.Context, c client.Client, ownedKeys []string) error
	Object() client.Object
}

// patchReplicasAndAnnotations applies a MergePatch with replicas and annotations.
// Annotations present in the map are set; keys in ownedKeys absent from the
// map are explicitly set to null so the merge patch deletes them server-side.
func patchReplicasAndAnnotations(ctx context.Context, c client.Client, obj client.Object, replicas *int32, annotations map[string]string, ownedKeys []string) error {
	annoPatch := make(map[string]interface{}, len(annotations)+len(ownedKeys))
	for k, v := range annotations {
		annoPatch[k] = v
	}
	for _, key := range ownedKeys {
		if _, exists := annotations[key]; !exists {
			annoPatch[key] = nil
		}
	}
	patch, err := json.Marshal(map[string]interface{}{
		"spec":     map[string]interface{}{"replicas": replicas},
		"metadata": map[string]interface{}{"annotations": annoPatch},
	})
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	return c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
}

// selectorFromLabelSelector converts a LabelSelector to a labels.Selector,
// returning labels.Nothing() on nil or error.
func selectorFromLabelSelector(ls *metav1.LabelSelector) labels.Selector {
	if ls == nil {
		return labels.Nothing()
	}
	sel, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		return labels.Nothing()
	}
	return sel
}

// RolloutWorkload wraps an Argo Rollout.
type RolloutWorkload struct {
	Rollout *rolloutsv1alpha1.Rollout
}

func (r *RolloutWorkload) GetReplicas() int32 {
	if r.Rollout.Spec.Replicas == nil {
		return 1
	}
	return *r.Rollout.Spec.Replicas
}

func (r *RolloutWorkload) SetReplicas(n int32) {
	r.Rollout.Spec.Replicas = &n
}

func (r *RolloutWorkload) IsStable() bool {
	return r.Rollout.Status.Phase == rolloutsv1alpha1.RolloutPhaseHealthy
}

// argoRestartingMessage is the status.message Argo Rollouts emits while a
// restart is pending. Set exclusively in upstream's CalculateRolloutPhase when
// spec.RestartAt != nil && status.RestartedAt has not caught up
// (utils/rollout/rolloututil.go in argoproj/argo-rollouts).
const argoRestartingMessage = "rollout is restarting"

// IsStableForRestart relaxes IsStable for the restart-surge path: Argo flips
// status.phase to Progressing the moment spec.restartAt is set, so requiring
// Healthy would be self-defeating. We accept Healthy, or Progressing with the
// exact restart message — which upstream only sets for pending restarts, not
// for user-driven deploys.
func (r *RolloutWorkload) IsStableForRestart() bool {
	if r.Rollout.Status.Phase == rolloutsv1alpha1.RolloutPhaseHealthy {
		return true
	}
	return r.Rollout.Status.Phase == rolloutsv1alpha1.RolloutPhaseProgressing &&
		r.Rollout.Status.Message == argoRestartingMessage
}

func (r *RolloutWorkload) CanSurge() bool { return true }

func (r *RolloutWorkload) GetPodSelector() labels.Selector {
	return selectorFromLabelSelector(r.Rollout.Spec.Selector)
}

func (r *RolloutWorkload) GetObjectMeta() *metav1.ObjectMeta {
	return &r.Rollout.ObjectMeta
}

func (r *RolloutWorkload) GetObjectKind() string { return "Rollout" }

func (r *RolloutWorkload) PatchOwned(ctx context.Context, c client.Client, ownedKeys []string) error {
	return patchReplicasAndAnnotations(ctx, c, r.Rollout, r.Rollout.Spec.Replicas, r.Rollout.Annotations, ownedKeys)
}

func (r *RolloutWorkload) Object() client.Object { return r.Rollout }

// DeploymentWorkload wraps a Kubernetes Deployment.
type DeploymentWorkload struct {
	Deployment *appsv1.Deployment
}

func (d *DeploymentWorkload) GetReplicas() int32 {
	if d.Deployment.Spec.Replicas == nil {
		return 1
	}
	return *d.Deployment.Spec.Replicas
}

func (d *DeploymentWorkload) SetReplicas(n int32) {
	d.Deployment.Spec.Replicas = &n
}

func (d *DeploymentWorkload) IsStable() bool {
	for _, cond := range d.Deployment.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing && cond.Reason == reasonNewRSAvailable {
			return true
		}
	}
	return d.Deployment.Status.ObservedGeneration == d.Deployment.Generation
}

// CanSurge returns false for Recreate strategy Deployments.
func (d *DeploymentWorkload) CanSurge() bool {
	return d.Deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType
}

func (d *DeploymentWorkload) GetPodSelector() labels.Selector {
	return selectorFromLabelSelector(d.Deployment.Spec.Selector)
}

func (d *DeploymentWorkload) GetObjectMeta() *metav1.ObjectMeta {
	return &d.Deployment.ObjectMeta
}

func (d *DeploymentWorkload) GetObjectKind() string { return "Deployment" }

func (d *DeploymentWorkload) PatchOwned(ctx context.Context, c client.Client, ownedKeys []string) error {
	return patchReplicasAndAnnotations(ctx, c, d.Deployment, d.Deployment.Spec.Replicas, d.Deployment.Annotations, ownedKeys)
}

func (d *DeploymentWorkload) Object() client.Object { return d.Deployment }
