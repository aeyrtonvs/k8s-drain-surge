package controller

import (
	"time"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// isRestartStuck reports whether an Argo Rollout has a pending restart
// (spec.restartAt set, status.restartedAt not yet matching) that has elapsed
// the grace period and still has pods older than spec.restartAt.
//
// Argo's PodRestarter goes via the eviction API and retries every 30s without
// emitting Events, Conditions, or status messages on PDB rejection — there is
// no first-class "stuck" signal in the Rollout API. The grace period prevents
// false positives on clusters where the PDB permits the eviction and Argo
// completes the restart on its own within a cycle or two.
func isRestartStuck(ro *rolloutsv1alpha1.Rollout, pods []corev1.Pod, gracePeriod time.Duration, now time.Time) bool {
	if ro.Spec.RestartAt == nil {
		return false
	}
	restartAt := ro.Spec.RestartAt.Time

	if ro.Status.RestartedAt != nil && ro.Status.RestartedAt.Equal(ro.Spec.RestartAt) {
		return false
	}

	if now.Before(restartAt) {
		return false
	}

	if now.Sub(restartAt) < gracePeriod {
		return false
	}

	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.CreationTimestamp.Time.Before(restartAt) {
			return true
		}
	}
	return false
}

// restartCompletedByArgo reports whether Argo has finished the restart for
// the given Rollout: either status.restartedAt matches spec.restartAt, or
// no remaining pods predate spec.restartAt.
func restartCompletedByArgo(ro *rolloutsv1alpha1.Rollout, pods []corev1.Pod) bool {
	if ro.Spec.RestartAt == nil {
		return true
	}
	if ro.Status.RestartedAt != nil && ro.Status.RestartedAt.Equal(ro.Spec.RestartAt) {
		return true
	}
	restartAt := ro.Spec.RestartAt.Time
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.CreationTimestamp.Time.Before(restartAt) {
			return false
		}
	}
	return true
}
