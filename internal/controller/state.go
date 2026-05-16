package controller

import (
	"fmt"
	"strconv"
	"time"
)

const ControllerName = "k8s-drain-surge"

type DrainState string

const (
	DrainStateNone     DrainState = ""
	DrainStatePending  DrainState = "pending"
	DrainStateScaledUp DrainState = "scaled-up"
	DrainStateReady    DrainState = "ready"
	DrainStateDraining DrainState = "draining"
	DrainStateDone     DrainState = "done"
)

const (
	AnnotationEnabled                = "k8s-drain-surge.io/enabled"
	AnnotationDrainState             = "k8s-drain-surge.io/drain-state"
	AnnotationOriginalReplicas       = "k8s-drain-surge.io/original-replicas"
	AnnotationDrainNode              = "k8s-drain-surge.io/drain-node"
	AnnotationDrainStart             = "k8s-drain-surge.io/drain-start"
	AnnotationHPAName                = "k8s-drain-surge.io/hpa-name"
	AnnotationHPAOriginalMinReplicas = "k8s-drain-surge.io/hpa-original-min-replicas"

	// Restart-surge annotations. Parallel to drain annotations; the two
	// operations are mutually exclusive on a given workload (state machine
	// guards prevent overlap), but use disjoint state keys so it is always
	// clear which operation owns the workload.
	AnnotationRestartSurgeState = "k8s-drain-surge.io/restart-surge-state"
	AnnotationRestartSurgeStart = "k8s-drain-surge.io/restart-surge-start"
)

const reasonNewRSAvailable = "NewReplicaSetAvailable"

// drainAnnotationKeys enumerates every annotation this controller owns.
// patchReplicasAndAnnotations nullifies any key in this slice that is
// absent from a given patch, so omissions silently delete on-disk state.
var drainAnnotationKeys = []string{
	AnnotationDrainState,
	AnnotationOriginalReplicas,
	AnnotationDrainNode,
	AnnotationDrainStart,
	AnnotationHPAName,
	AnnotationHPAOriginalMinReplicas,
	AnnotationRestartSurgeState,
	AnnotationRestartSurgeStart,
}

// restartSurgeFullKeys are cleared on a successful restart-surge cycle or
// full abort: state plus shared bookkeeping the operation owned while in
// flight (original replicas, HPA pointer/min).
var restartSurgeFullKeys = []string{
	AnnotationRestartSurgeState,
	AnnotationRestartSurgeStart,
	AnnotationOriginalReplicas,
	AnnotationHPAName,
	AnnotationHPAOriginalMinReplicas,
}

// restartSurgeExclusiveKeys hold only the restart-surge state markers,
// excluding shared bookkeeping. Used when yielding to a drain that started
// mid-operation so the drain's shared keys stay intact.
var restartSurgeExclusiveKeys = []string{
	AnnotationRestartSurgeState,
	AnnotationRestartSurgeStart,
}

func clearDrainAnnotations(annotations map[string]string) {
	for _, key := range drainAnnotationKeys {
		delete(annotations, key)
	}
}

func clearRestartSurgeAnnotations(annotations map[string]string) {
	for _, key := range restartSurgeFullKeys {
		delete(annotations, key)
	}
}

func clearRestartSurgeExclusiveAnnotations(annotations map[string]string) {
	for _, key := range restartSurgeExclusiveKeys {
		delete(annotations, key)
	}
}

func parseDrainStart(annotations map[string]string) (time.Time, bool) {
	return parseRFC3339Annotation(annotations, AnnotationDrainStart)
}

func parseRestartSurgeStart(annotations map[string]string) (time.Time, bool) {
	return parseRFC3339Annotation(annotations, AnnotationRestartSurgeStart)
}

func parseRFC3339Annotation(annotations map[string]string, key string) (time.Time, bool) {
	s, ok := annotations[key]
	if !ok || s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// getOriginalReplicas parses the original replica count from annotations.
// Returns the value and an error if the annotation is missing or invalid.
func getOriginalReplicas(annotations map[string]string) (int32, error) {
	s, ok := annotations[AnnotationOriginalReplicas]
	if !ok || s == "" {
		return 0, fmt.Errorf("annotation %s not found", AnnotationOriginalReplicas)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", AnnotationOriginalReplicas, s, err)
	}
	return int32(n), nil
}

// workloadKey returns a deduplciation key for a workload (namespace/name).
func workloadKey(namespace, name string) string {
	return namespace + "/" + name
}
