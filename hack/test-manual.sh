#!/usr/bin/env bash
set -euo pipefail

# k8s-drain-surge Manual Test Script

NAMESPACE="${NAMESPACE:-default}"
HELM_RELEASE="${HELM_RELEASE:-k8s-drain-surge}"
HELM_CHART="${HELM_CHART:-deploy/helm/k8s-drain-surge}"
DRAIN_TAINT="${DRAIN_TAINT:-karpenter.sh/disrupted}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
step()  { echo -e "\n${GREEN}=== STEP $1 ===${NC} $2"; }
pause() { echo -e "${YELLOW}Press Enter to continue...${NC}"; read -r; }

check_annotations() {
  local kind=$1 name=$2
  echo "Annotations:"
  kubectl get "$kind" "$name" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations}' | python3 -m json.tool 2>/dev/null || \
    kubectl get "$kind" "$name" -n "$NAMESPACE" -o jsonpath='{.metadata.annotations}'
  echo
}

check_replicas() {
  local kind=$1 name=$2
  echo "Replicas: $(kubectl get "$kind" "$name" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}')"
}

check_pods() {
  local selector=$1
  kubectl get pods -l "$selector" -n "$NAMESPACE" -o wide
}

check_events() {
  local name=$1
  kubectl get events --field-selector "involvedObject.name=$name" -n "$NAMESPACE" --sort-by='.lastTimestamp' | tail -10
}

controller_logs() {
  kubectl logs -l app.kubernetes.io/name=k8s-drain-surge -n "$NAMESPACE" --tail=20
}

get_pod_node() {
  local selector=$1
  kubectl get pods -l "$selector" -n "$NAMESPACE" -o jsonpath='{.items[0].spec.nodeName}'
}

taint_node() {
  local node=$1
  kubectl taint node "$node" "${DRAIN_TAINT}=:NoSchedule" --overwrite
}

untaint_node() {
  local node=$1
  kubectl taint node "$node" "${DRAIN_TAINT}-" 2>/dev/null || true
}

# run_drain_test: shared flow for testing a workload type
# Args: kind name selector node_var
run_drain_test() {
  local kind=$1 name=$2 selector=$3 node=$4

  info "Tainting node: $node"
  taint_node "$node"
  sleep 15

  check_annotations "$kind" "$name"
  check_replicas "$kind" "$name"
  check_pods "$selector"
  pause

  info "Waiting for new pod to be ready..."
  kubectl wait --for=condition=ready pod -l "$selector" -n "$NAMESPACE" --timeout=120s || true
  check_annotations "$kind" "$name"

  OLD_POD=$(kubectl get pods -l "$selector" -n "$NAMESPACE" --field-selector "spec.nodeName=$node" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$OLD_POD" ]; then
    kubectl delete pod "$OLD_POD" -n "$NAMESPACE"
  else
    warn "No pod found on $node — it may have already been evicted"
  fi
  sleep 10

  check_annotations "$kind" "$name"
  check_replicas "$kind" "$name"
  check_events "$name"
  untaint_node "$node"
  pause
}

# ============================================================================
# PHASE 1: Setup
# ============================================================================
step "1" "Install controller via Helm"
info "Installing $HELM_RELEASE..."
helm upgrade --install "$HELM_RELEASE" "$HELM_CHART" -n "$NAMESPACE" --wait
pause

step "2" "Apply test fixtures"
kubectl apply -f hack/test-rollout-canary.yaml -n "$NAMESPACE"
kubectl apply -f hack/test-rollout-bluegreen.yaml -n "$NAMESPACE"
kubectl apply -f hack/test-deployment.yaml -n "$NAMESPACE"
info "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app=test-canary -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=ready pod -l app=test-bluegreen -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=ready pod -l app=test-deployment -n "$NAMESPACE" --timeout=120s
pause

step "3" "Verify pods running and ready"
check_pods "app=test-canary"
check_pods "app=test-bluegreen"
check_pods "app=test-deployment"
pause

step "4" "Note pod nodes"
CANARY_NODE=$(get_pod_node "app=test-canary")
BLUEGREEN_NODE=$(get_pod_node "app=test-bluegreen")
DEPLOYMENT_NODE=$(get_pod_node "app=test-deployment")
info "Canary pod on node: $CANARY_NODE"
info "Blue-Green pod on node: $BLUEGREEN_NODE"
info "Deployment pod on node: $DEPLOYMENT_NODE"
pause

# ============================================================================
# PHASE 2: Test Canary Rollout
# ============================================================================
step "5-14" "Test Canary Rollout"
run_drain_test "rollout" "test-canary" "app=test-canary" "$CANARY_NODE"

# ============================================================================
# PHASE 3: Test Blue-Green Rollout
# ============================================================================
step "15-24" "Test Blue-Green Rollout"
run_drain_test "rollout" "test-bluegreen" "app=test-bluegreen" "$BLUEGREEN_NODE"

step "25" "Verify active service always had endpoints"
kubectl get endpoints test-bluegreen-active -n "$NAMESPACE"
pause

# ============================================================================
# PHASE 4: Test Deployment
# ============================================================================
step "26-35" "Test Deployment"
run_drain_test "deployment" "test-deployment" "app=test-deployment" "$DEPLOYMENT_NODE"

step "36" "Verify service always had endpoints"
kubectl get endpoints test-deployment -n "$NAMESPACE"
pause

# ============================================================================
# PHASE 5: Test abort by timeout
# ============================================================================
step "37-40" "Test abort by timeout"
info "Creating a workload with an image that will never pass readiness..."
cat <<'YAML' | kubectl apply -n "$NAMESPACE" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-timeout
  annotations:
    k8s-drain-surge.io/enabled: "true"
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: test-timeout
  template:
    metadata:
      labels:
        app: test-timeout
    spec:
      containers:
        - name: fake
          image: nginx:1.25
          readinessProbe:
            httpGet:
              path: /nonexistent
              port: 12345
            initialDelaySeconds: 1
            periodSeconds: 2
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: test-timeout-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: test-timeout
YAML
kubectl wait --for=condition=ready pod -l app=test-timeout -n "$NAMESPACE" --timeout=60s || warn "Pod may not become fully ready (expected for this test)"
TIMEOUT_NODE=$(get_pod_node "app=test-timeout")
info "Tainting node: $TIMEOUT_NODE"
taint_node "$TIMEOUT_NODE"
warn "Now wait for the readiness-timeout to expire. Check controller logs periodically."
warn "This may take up to the configured --readiness-timeout (default 10m)."
info "You can watch with: kubectl logs -f -l app.kubernetes.io/name=k8s-drain-surge -n $NAMESPACE"
pause

check_annotations "deployment" "test-timeout"
check_replicas "deployment" "test-timeout"
check_events "test-timeout"
untaint_node "$TIMEOUT_NODE"
kubectl delete deployment test-timeout -n "$NAMESPACE" || true
kubectl delete pdb test-timeout-pdb -n "$NAMESPACE" || true
pause

# ============================================================================
# PHASE 6: Test abort by taint removed
# ============================================================================
step "41-44" "Test abort by taint removal"
CANARY_NODE=$(get_pod_node "app=test-canary")
info "Tainting node: $CANARY_NODE"
taint_node "$CANARY_NODE"
sleep 10
info "Current state:"
check_annotations "rollout" "test-canary"

info "Removing taint immediately..."
untaint_node "$CANARY_NODE"
sleep 10

info "Verifying abort:"
check_annotations "rollout" "test-canary"
check_replicas "rollout" "test-canary"
pause

# ============================================================================
# PHASE 8: Cleanup
# ============================================================================
step "48-49" "Cleanup"
info "Removing test fixtures..."
kubectl delete -f hack/test-rollout-canary.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete -f hack/test-rollout-bluegreen.yaml -n "$NAMESPACE" --ignore-not-found
kubectl delete -f hack/test-deployment.yaml -n "$NAMESPACE" --ignore-not-found

info "Uninstalling controller..."
helm uninstall "$HELM_RELEASE" -n "$NAMESPACE" || true

info "Removing any leftover taints..."
for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  untaint_node "$node"
done

echo
info "All tests complete!"
