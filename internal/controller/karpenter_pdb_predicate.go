package controller

import (
	"context"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// karpenterPDBPredicate accepts only PDB events that represent a transition
// into the blocked state. Specifically:
//   - Create: PDB is born with disruptionsAllowed == 0 (e.g. controller
//     started after the block occurred).
//   - Update: disruptionsAllowed went from >0 to 0.
//
// Everything else is rejected. The reconciler picks up unblock transitions
// via its own polling once a karpenter-surge is active.
func karpenterPDBPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			p, ok := e.Object.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			return isPDBBlocked(p)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldP, ok := e.ObjectOld.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			newP, ok := e.ObjectNew.(*policyv1.PodDisruptionBudget)
			if !ok {
				return false
			}
			return oldP.Status.DisruptionsAllowed > 0 && isPDBBlocked(newP)
		},
		DeleteFunc:  func(_ event.DeleteEvent) bool { return false },
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// mapPDBToWorkload resolves a PDB to ALL Rollouts/Deployments that own any
// of its selected pods. A single PDB may legitimately cover multiple
// workloads via a broad selector (FindMatchingPDB's subset semantics
// explicitly support this), so we must enqueue every matching workload;
// otherwise siblings starve until the next backup-scanner tick. Requests
// are deduped by NamespacedName.
func (r *KarpenterSurgeReconciler) mapPDBToWorkload(ctx context.Context, obj client.Object) []reconcile.Request {
	pdb, ok := obj.(*policyv1.PodDisruptionBudget)
	if !ok {
		return nil
	}
	if pdb.Spec.Selector == nil {
		return nil
	}
	sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return nil
	}
	pods, err := listMatchingPods(ctx, r.Client, pdb.Namespace, sel)
	if err != nil {
		log.FromContext(ctx).V(1).Info("mapPDBToWorkload: failed to list pods", LogFieldKarpenterPDB, pdb.Namespace+"/"+pdb.Name)
		return nil
	}
	seen := make(map[types.NamespacedName]struct{}, 4)
	var out []reconcile.Request
	for i := range pods {
		wl, err := ResolveWorkloadFromPod(ctx, r.Client, &pods[i])
		if err != nil || wl == nil {
			continue
		}
		meta := wl.GetObjectMeta()
		key := types.NamespacedName{Namespace: meta.Namespace, Name: meta.Name}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, reconcile.Request{NamespacedName: key})
	}
	return out
}

// karpenterPodPredicate would have funneled every Pod event in the cluster
// through ResolveWorkloadFromPod (two cache GETs per event) — an unjustified
// cost in large clusters where most pods do not belong to opted-in
// workloads. Removed in favor of: (a) the PDB watch as the primary trigger,
// (b) the backup scanner as a periodic safety net, and (c) the active
// reconciler's requeue loop (RequeueInterval=5s) to observe surge-pod
// readiness via direct List inside handleKarpenterWaitReady. Retained as a
// stub so call sites that referenced it still compile; SetupWithManager no
// longer registers a Pod watch.
func karpenterPodPredicate() predicate.Predicate {
	reject := func(client.Object) bool { return false }
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return reject(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return reject(e.ObjectNew) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return reject(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return reject(e.Object) },
	}
}
