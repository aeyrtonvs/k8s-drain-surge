package controller

import (
	"context"
	"fmt"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResolveWorkloadFromPod walks the ownerReference chain: Pod -> ReplicaSet -> Rollout|Deployment.
// Returns nil if no supported workload is found.
func ResolveWorkloadFromPod(ctx context.Context, c client.Client, pod *corev1.Pod) (DrainableWorkload, error) {
	var rsName string
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "ReplicaSet" && ref.APIVersion == "apps/v1" {
			rsName = ref.Name
			break
		}
	}
	if rsName == "" {
		return nil, nil
	}

	var rs appsv1.ReplicaSet
	if err := c.Get(ctx, types.NamespacedName{Name: rsName, Namespace: pod.Namespace}, &rs); err != nil {
		return nil, fmt.Errorf("get ReplicaSet %s/%s: %w", pod.Namespace, rsName, err)
	}

	for _, ref := range rs.OwnerReferences {
		switch {
		case ref.Kind == "Rollout" && ref.APIVersion == "argoproj.io/v1alpha1":
			var rollout rolloutsv1alpha1.Rollout
			if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: pod.Namespace}, &rollout); err != nil {
				return nil, fmt.Errorf("get Rollout %s/%s: %w", pod.Namespace, ref.Name, err)
			}
			return &RolloutWorkload{Rollout: &rollout}, nil

		case ref.Kind == "Deployment" && ref.APIVersion == "apps/v1":
			var dep appsv1.Deployment
			if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: pod.Namespace}, &dep); err != nil {
				return nil, fmt.Errorf("get Deployment %s/%s: %w", pod.Namespace, ref.Name, err)
			}
			return &DeploymentWorkload{Deployment: &dep}, nil
		}
	}

	return nil, nil
}

// FindMatchingPDB returns true if there exists a PDB whose selector matches the workload's pods.
// It checks that the PDB selector requirements are a subset of (or equal to) the workload's
// pod selector requirements, so PDBs with matching labels are correctly detected.
func FindMatchingPDB(ctx context.Context, c client.Client, namespace string, podSelector labels.Selector) (bool, error) {
	var pdbList policyv1.PodDisruptionBudgetList
	if err := c.List(ctx, &pdbList, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("list PDBs: %w", err)
	}

	workloadReqs, _ := podSelector.Requirements()

	for i := range pdbList.Items {
		pdb := &pdbList.Items[i]
		if pdb.Spec.Selector == nil {
			continue
		}
		pdbSelector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		// A PDB covers the workload's pods if every requirement in the PDB selector
		// also appears in the workload selector (i.e. the PDB selects a superset of
		// or the same pods as the workload). In the common case both selectors use
		// identical matchLabels.
		pdbReqs, _ := pdbSelector.Requirements()
		if requirementsMatch(pdbReqs, workloadReqs) {
			return true, nil
		}
	}
	return false, nil
}

// requirementsMatch returns true if every requirement in pdbReqs has a matching
// requirement in workloadReqs (same key, operator, and values).
func requirementsMatch(pdbReqs, workloadReqs []labels.Requirement) bool {
	if len(pdbReqs) == 0 {
		return false
	}
	for _, pr := range pdbReqs {
		found := false
		for _, wr := range workloadReqs {
			if pr.String() == wr.String() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// CheckHPACompatibility checks if an HPA targets the workload and if maxReplicas allows surge.
// Returns (compatible, hpaExists, error).
func CheckHPACompatibility(ctx context.Context, c client.Client, namespace, workloadName, workloadKind string) (bool, bool, error) {
	var hpaList autoscalingv2.HorizontalPodAutoscalerList
	if err := c.List(ctx, &hpaList, client.InNamespace(namespace)); err != nil {
		return false, false, fmt.Errorf("list HPAs: %w", err)
	}

	for i := range hpaList.Items {
		hpa := &hpaList.Items[i]
		ref := hpa.Spec.ScaleTargetRef
		if ref.Name == workloadName && ref.Kind == workloadKind {
			return hpa.Spec.MaxReplicas > 1, true, nil
		}
	}
	return true, false, nil
}

// FindReadyPodOnOtherNode returns true if there is a Ready pod matching the selector
// running on a node other than the specified drain node.
func FindReadyPodOnOtherNode(ctx context.Context, c client.Client, namespace string, podSelector labels.Selector, drainNode string) (bool, error) {
	pods, err := listMatchingPods(ctx, c, namespace, podSelector)
	if err != nil {
		return false, err
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName != drainNode && isPodReady(pod) && !isPodTerminating(pod) {
			return true, nil
		}
	}
	return false, nil
}

// FindPodOnNode returns true if there is a non-terminated pod matching the selector on the given node.
func FindPodOnNode(ctx context.Context, c client.Client, namespace string, podSelector labels.Selector, nodeName string) (bool, error) {
	pods, err := listMatchingPods(ctx, c, namespace, podSelector)
	if err != nil {
		return false, err
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == nodeName && !isPodTerminating(pod) && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return true, nil
		}
	}
	return false, nil
}

func listMatchingPods(ctx context.Context, c client.Client, namespace string, podSelector labels.Selector) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := c.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: podSelector}); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	return podList.Items, nil
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isPodTerminating(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil
}
