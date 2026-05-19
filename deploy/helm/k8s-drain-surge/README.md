# k8s-drain-surge

**Zero-downtime node drains and restarts for single-replica Deployments and Argo Rollouts in Kubernetes.**

This chart deploys the controller that watches for drain taints and PDB-blocked Argo Rollout restarts, and pre-empts downtime by temporarily surging affected workloads from 1 to 2 replicas.

For architecture, troubleshooting, and contributing, see the [project README](https://github.com/aeyrtonvs/k8s-drain-surge).

## TL;DR

```bash
helm install k8s-drain-surge oci://ghcr.io/aeyrtonvs/charts/k8s-drain-surge \
  --namespace kube-system
```

Then opt in any single-replica workload and pair it with a PDB:

```yaml
metadata:
  annotations:
    k8s-drain-surge.io/enabled: "true"
---
apiVersion: policy/v1
kind: PodDisruptionBudget
spec:
  minAvailable: 1
  selector: { matchLabels: { app: my-app } }
```

## Prerequisites

- Kubernetes >= 1.28
- A `PodDisruptionBudget` for each workload you want protected (required — the controller skips workloads without one)
- One of the following node lifecycle managers (or manual drain):
  - [Karpenter](https://karpenter.sh/) >= 0.32
  - [Cluster Autoscaler](https://github.com/kubernetes/autoscaler)
  - Manual `kubectl drain` / `kubectl cordon` / `kubectl taint`
- [Argo Rollouts](https://argoproj.github.io/argo-rollouts/) >= 1.5 (only if you enable restart-surge)

## Installation

### From the OCI registry

```bash
helm install k8s-drain-surge oci://ghcr.io/aeyrtonvs/charts/k8s-drain-surge \
  --version <VERSION> \
  --namespace kube-system
```

### From the repo source

```bash
git clone https://github.com/aeyrtonvs/k8s-drain-surge.git
helm install k8s-drain-surge ./k8s-drain-surge/deploy/helm/k8s-drain-surge \
  --namespace kube-system
```

### Enabling restart-surge (Argo Rollouts only)

Off by default. Enable it when you run Argo Rollouts and want to unblock single-replica restarts that the PDB rejects:

```bash
helm install k8s-drain-surge oci://ghcr.io/aeyrtonvs/charts/k8s-drain-surge \
  --namespace kube-system \
  --set controller.restartSurge.enabled=true
```

## Verifying signatures

Release artifacts (controller image and Helm chart) are signed with [cosign](https://docs.sigstore.dev/) using GitHub Actions OIDC keyless signing. To verify before installing:

```bash
# Image
cosign verify ghcr.io/aeyrtonvs/k8s-drain-surge:<VERSION> \
  --certificate-identity-regexp="^https://github.com/aeyrtonvs/k8s-drain-surge/\.github/workflows/release\.yaml@refs/tags/release/.*$" \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com

# Chart
cosign verify ghcr.io/aeyrtonvs/charts/k8s-drain-surge:<VERSION> \
  --certificate-identity-regexp="^https://github.com/aeyrtonvs/k8s-drain-surge/\.github/workflows/release\.yaml@refs/tags/release/.*$" \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com
```

## Opting workloads in

For each workload you want protected:

```yaml
# Deployment or Argo Rollout
metadata:
  annotations:
    k8s-drain-surge.io/enabled: "true"
spec:
  replicas: 1
  # Deployments only: must be RollingUpdate (Recreate is skipped)
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
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

### ArgoCD / FluxCD

Add `ignoreDifferences` so the GitOps controller does not reset `spec.replicas` during the surge:

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

### Karpenter `terminationGracePeriod`

If your `NodePool` configures `terminationGracePeriod`, it must be **greater** than `controller.readinessTimeout` (default 10m). Without it, Karpenter waits indefinitely — which is the ideal behavior.

## Configuration

Common values. The full schema, including types and constraints, lives in [`values.schema.json`](./values.schema.json) (Helm validates `--set` flags against it).

| Value | Default | Description |
|---|---|---|
| `replicaCount` | `2` | Controller replicas (HA with leader election) |
| `image.repository` | `ghcr.io/aeyrtonvs/k8s-drain-surge` | Controller image |
| `image.tag` | `""` (uses `Chart.AppVersion`) | Override image tag |
| `controller.readinessTimeout` | `10m` | Time to wait for a surged pod to become Ready before aborting. Windows workloads may need 15m+ |
| `controller.requeueInterval` | `5s` | Poll interval between state-machine reconciles |
| `controller.leaderElect` | `true` | Enable leader election (required when `replicaCount > 1`) |
| `controller.metricsPort` | `8080` | Prometheus metrics bind port |
| `controller.healthPort` | `8081` | Liveness/readiness probe bind port |
| `controller.restartSurge.enabled` | `false` | Enable restart-surge protection (Argo Rollouts only) |
| `controller.restartSurge.gracePeriod` | `60s` | Wait this long after `spec.restartAt` before surging — lets Argo finish on its own when PDB permits |
| `controller.restartSurge.timeout` | `10m` | Total budget for one restart-surge operation |
| `priorityClassName` | `system-cluster-critical` | Pod `priorityClassName` |
| `nodeSelector` | `{ kubernetes.io/os: linux }` | Node selector for controller pods |
| `resources.requests` | `cpu 10m, memory 64Mi` | A reactive controller idles most of the time; the default is sized for that |
| `resources.limits` | `memory 128Mi` (no CPU limit) | No CPU limit by design — throttling a reactive controller hurts reconcile latency during the moments it matters most |

### Adjusting for Windows workloads

Windows pods take longer to start (12–17 min cold-start is normal). Raise the readiness timeout:

```bash
helm install k8s-drain-surge oci://ghcr.io/aeyrtonvs/charts/k8s-drain-surge \
  --namespace kube-system \
  --set controller.readinessTimeout=15m
```

## Uninstallation

```bash
helm uninstall k8s-drain-surge --namespace kube-system
```

The controller does not own any cluster-scoped resources beyond its `ClusterRole` and `ClusterRoleBinding`, which Helm removes. Opt-in annotations on your workloads stay — remove them manually if you no longer want the controller to handle them on a future install:

```bash
kubectl annotate deployment my-app k8s-drain-surge.io/enabled-
```

## Source

- Project: https://github.com/aeyrtonvs/k8s-drain-surge
- Issues: https://github.com/aeyrtonvs/k8s-drain-surge/issues
- License: [Apache-2.0](https://github.com/aeyrtonvs/k8s-drain-surge/blob/master/LICENSE)
