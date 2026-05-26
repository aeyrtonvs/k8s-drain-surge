package controller

// Structured log field keys. Centralized so downstream consumers (Loki,
// Datadog, etc.) can rely on a stable vocabulary and typos are caught at
// compile time.
const (
	LogFieldWorkload    = "workload"
	LogFieldNamespace   = "namespace"
	LogFieldKind        = "kind"
	LogFieldNode        = "node"
	LogFieldOtherNode   = "otherNode"
	LogFieldState       = "state"
	LogFieldHPA         = "hpa"
	LogFieldFrom        = "from"
	LogFieldTo          = "to"
	LogFieldReplicas    = "replicas"
	LogFieldMaxReplicas = "maxReplicas"
	LogFieldMinReplicas = "minReplicas"
	LogFieldPod         = "pod"
	LogFieldRollout     = "rollout"
	LogFieldDeployment  = "deployment"
	LogFieldDrainNode   = "drainNode"
	LogFieldRestartAt   = "restartAt"

	LogFieldElapsed     = "elapsed"
	LogFieldGracePeriod = "gracePeriod"
	LogFieldRemaining   = "remaining"

	LogFieldRolloutPhase   = "rolloutPhase"
	LogFieldRolloutMessage = "rolloutMessage"
)

// Field index keys registered on the controller-runtime cache.
const (
	IndexPodNodeName       = "spec.nodeName"
	IndexWorkloadDrainNode = "metadata.annotations." + AnnotationDrainNode
)
