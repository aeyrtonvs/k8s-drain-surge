# k8s-drain-surge

Zero-downtime node drain controller for single-replica workloads in Kubernetes.

When a node is tainted for drain (by Karpenter, Cluster Autoscaler, or manual `kubectl drain`), this controller temporarily scales opted-in workloads from 1 to 2 replicas, waits for the new pod to be ready on a different node, and lets the eviction proceed safely.

Supports **Argo Rollouts** (Canary and Blue-Green) and **Deployments**.

## Dependencies

- Kubernetes >= 1.28
- A PodDisruptionBudget per workload (required)
- One of the following node lifecycle managers (or manual drain):
  - [Karpenter](https://karpenter.sh/) >= 0.32
  - [Cluster Autoscaler](https://github.com/kubernetes/autoscaler)
  - Manual `kubectl drain` / `kubectl taint`
- [Argo Rollouts](https://argoproj.github.io/argo-rollouts/) >= 1.5 (only if using Rollout workloads)

## How it works

The controller watches for nodes with drain taints and runs a state machine per workload:

```
1. Node gets tainted (e.g. karpenter.sh/disrupted)
2. Controller finds single-replica workloads on that node
3. Scales the workload 1 -> 2 (surge)
4. Waits for the new pod to be Ready on another node
5. PDB is now satisfied, Karpenter evicts the old pod
6. Controller scales back 2 -> 1
7. Cleans up annotations
```

The controller is **opt-in only**. It will not touch any workload unless you explicitly annotate it.

## What you need to do

For each workload you want protected:

### 1. Add the opt-in annotation

```yaml
metadata:
  annotations:
    k8s-drain-surge.io/enabled: "true"
```

### 2. Create a PodDisruptionBudget

The PDB is what blocks Karpenter from killing the pod before the surge is ready. Without it, the controller skips the workload.

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: my-app-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: my-app
```

### 3. Deployment strategy (Deployments only)

Deployments must use `RollingUpdate`. The controller skips `Recreate` strategy.

```yaml
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
```

### 4. ArgoCD / FluxCD users

If your workloads are managed by ArgoCD, add `ignoreDifferences` so ArgoCD does not reset `spec.replicas` during the surge:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
spec:
  ignoreDifferences:
    - group: argoproj.io
      kind: Rollout
      jsonPointers:
        - /spec/replicas
    - group: apps
      kind: Deployment
      jsonPointers:
        - /spec/replicas
  syncPolicy:
    syncOptions:
      - RespectIgnoreDifferences=true
```

### 5. Karpenter NodePool (if applicable)

If your NodePool has `terminationGracePeriod` configured, it must be **greater** than the controller's `--readiness-timeout` (default 10m). Without `terminationGracePeriod`, Karpenter waits indefinitely (which is the ideal behavior).

## Development

### Devcontainer (recommended)

Requires VS Code with the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension, or any editor that supports the devcontainer spec.

1. Open the project in VS Code
2. `Cmd+Shift+P` → **Dev Containers: Reopen in Container**
3. The container starts with Go 1.22 and runs `go mod tidy` automatically

### Make targets

| Command | Description |
|---|---|
| `make build` | Compile the controller binary to `bin/controller` |
| `make test` | Run all tests with race detector |
| `make vet` | Run `go vet` |
| `make fmt` | Run `go fmt` |
| `make tidy` | Run `go mod tidy` |
| `make docker-build` | Build the Docker image locally |
| `make helm-package` | Package the Helm chart to `bin/` |

## Installation

### Helm CLI

```bash
helm install k8s-drain-surge oci://ghcr.io/aeyrtonvs/charts/k8s-drain-surge \
  --version <VERSION> \
  --namespace kube-system
```

Or from the repo source:

```bash
helm install k8s-drain-surge deploy/helm/k8s-drain-surge \
  --namespace kube-system
```

### Terraform

```hcl
resource "helm_release" "k8s_drain_surge" {
  name       = "k8s-drain-surge"
  namespace  = "kube-system"
  repository = "oci://ghcr.io/aeyrtonvs/charts"
  chart      = "k8s-drain-surge"
  version    = "<VERSION>"

  set {
    name  = "controller.readinessTimeout"
    value = "10m"
  }

  set {
    name  = "controller.requeueInterval"
    value = "5s"
  }
}
```

## Configuration

All configuration is done through the Helm `values.yaml`:

| Value | Default | Description |
|---|---|---|
| `replicaCount` | `2` | Controller replicas (HA with leader election) |
| `controller.readinessTimeout` | `10m` | Timeout waiting for new pod readiness |
| `controller.requeueInterval` | `5s` | Poll interval between state checks |
| `controller.leaderElect` | `true` | Enable leader election |
| `controller.metricsPort` | `8080` | Prometheus metrics port |
| `controller.healthPort` | `8081` | Health/readiness probe port |
| `priorityClassName` | `system-cluster-critical` | Pod priority class |
| `nodeSelector` | `kubernetes.io/os: linux` | Node selector for controller pods |

For Windows workloads that take longer to start, increase the timeout:

```bash
helm install k8s-drain-surge oci://ghcr.io/aeyrtonvs/charts/k8s-drain-surge \
  --namespace kube-system \
  --set controller.readinessTimeout=15m
```

## Full example: Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
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
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
        - name: my-app
          image: my-app:latest
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: my-app-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: my-app
```

## Full example: Argo Rollout

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: my-app
  annotations:
    k8s-drain-surge.io/enabled: "true"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
        - name: my-app
          image: my-app:latest
  strategy:
    canary:
      steps:
        - setWeight: 20
        - pause: { duration: 30s }
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: my-app-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: my-app
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Controller skips workload | Missing `k8s-drain-surge.io/enabled: "true"` annotation | Add the annotation |
| "no matching PDB" in logs | No PDB with matching selector | Create a PDB with `minAvailable: 1` |
| "not stable" in logs | Workload is mid-rollout | Wait for rollout to complete |
| "strategy does not support surge" | Deployment uses `Recreate` strategy | Change to `RollingUpdate` |
| "HPA maxReplicas=1" | HPA prevents scaling above 1 | Set `maxReplicas >= 2` or remove HPA |
| Replicas stuck at 2 | Controller crashed mid-operation | Restarts recover automatically; manual fix below |
| Timeout on Windows pods | Windows nodes take 12-17min to start | Set `--readiness-timeout 15m` or higher |

Manual cleanup if replicas are stuck:

```bash
kubectl annotate deployment my-app \
  k8s-drain-surge.io/drain-state- \
  k8s-drain-surge.io/original-replicas- \
  k8s-drain-surge.io/drain-node- \
  k8s-drain-surge.io/drain-start-
kubectl scale deployment my-app --replicas=1
```

## License

See [LICENSE](LICENSE).
