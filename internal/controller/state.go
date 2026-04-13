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
	AnnotationEnabled          = "k8s-drain-surge.io/enabled"
	AnnotationDrainState       = "k8s-drain-surge.io/drain-state"
	AnnotationOriginalReplicas = "k8s-drain-surge.io/original-replicas"
	AnnotationDrainNode        = "k8s-drain-surge.io/drain-node"
	AnnotationDrainStart       = "k8s-drain-surge.io/drain-start"
)

const reasonNewRSAvailable = "NewReplicaSetAvailable"

// drainAnnotationKeys lists all annotation keys managed by this controller,
// used for bulk cleanup.
var drainAnnotationKeys = []string{
	AnnotationDrainState,
	AnnotationOriginalReplicas,
	AnnotationDrainNode,
	AnnotationDrainStart,
}

// clearDrainAnnotations removes all controller-managed annotations from the map.
func clearDrainAnnotations(annotations map[string]string) {
	for _, key := range drainAnnotationKeys {
		delete(annotations, key)
	}
}

// parseDrainStart parses the drain start timestamp from annotations.
// Returns zero time and false if not present or invalid.
func parseDrainStart(annotations map[string]string) (time.Time, bool) {
	s, ok := annotations[AnnotationDrainStart]
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
