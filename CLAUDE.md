# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A Kubernetes controller (Go, controller-runtime) that prevents downtime for single-replica workloads during disruptive eviction events. Two protections:

1. **Drain-surge** (always on) — when a node gets a drain taint, scale opted-in `Deployment`s and Argo `Rollout`s from 1→2 replicas, wait for the surge pod Ready on a different node, let the eviction happen, scale back.
2. **Restart-surge** (opt-in via `--restart-surge-enabled`) — when an Argo Rollout's `spec.restartAt` is blocked by a PDB (Argo's `PodRestarter` uses the eviction API and a single-replica PDB rejects it indefinitely), surge 1→2 so the PDB permits the eviction, then scale back once Argo finishes.

Module: `github.com/aeyrtonvs/k8s-drain-surge`. Go 1.22. Builds against `k8s.io/*` v0.30 and `controller-runtime` v0.18.

## Dev environment

The repo ships with a VS Code devcontainer (`.devcontainer/`) so the toolchain (Go 1.22, make) is reproducible and CI-equivalent locally.

- **Image**: `golang:1.22-bookworm`. Go is preinstalled at `/usr/local/go/bin` and on `PATH` by default.
- **Persisted caches**: two named Docker volumes — `k8s-drain-surge-gomodcache` (`/go/pkg/mod`) and `k8s-drain-surge-gobuildcache` (`/root/.cache/go-build`) — so module/build state survives container rebuilds. Speeds up cold opens dramatically; safe to `docker volume rm` if ever corrupted.
- **`onCreateCommand`**: installs `make` via apt (the base image doesn't ship it).
- **`postCreateCommand`**: runs `.devcontainer/bootstrap.sh` once, which executes `go mod download` followed by the same `make` targets CI runs (`tidy`, `vet`, `test`, `build`). This verifies the workspace end-to-end before you start editing.

Usage:
- Open the repo in VS Code → "Reopen in Container". First open builds the image and runs bootstrap (a few minutes); subsequent opens are instant thanks to the cached volumes.
- Inside the container, the standard `make` targets all work directly. CI parity is the contract: anything green in the devcontainer should be green in CI.
- If bootstrap fails (e.g. a test broke on `master`), you still get a shell — fix it and re-run `bash .devcontainer/bootstrap.sh` manually.

The devcontainer is the supported local environment for tasks that need to compile or test Go (host machines without Go installed should use it rather than `brew install go`).

## Common commands

- `make build` — compile to `bin/controller`
- `make test` — `go test ./... -v -race` (always run with `-race`; CI does)
- `make vet` / `make fmt` / `make tidy`
- Run a single test: `go test ./internal/controller -run TestName -v -race`
- `make docker-build` — buildx image as `ghcr.io/aeyrtonvs/k8s-drain-surge:latest` (override with `IMG=` / `TAG=`)
- `make helm-package` — chart tarball to `bin/`; `helm lint deploy/helm/k8s-drain-surge` is run in CI

CI (`.github/workflows/ci.yaml`) runs: `go mod tidy`, `go vet`, `go test -race`, build, docker build (no push), `helm lint`. Match these locally before pushing.

## Architecture

### Entry point and reconciler topology

`cmd/controller/main.go` wires a controller-runtime manager with two reconcilers:

- `NodeReconciler` (`internal/controller/node_controller.go`) — always on. Handles node drains. Keyed on `Node`, watches `Node`, `Pod` (mapped to `spec.nodeName`), and `Rollout`/`Deployment` (mapped to their `AnnotationDrainNode`). A field index on `Pod.spec.nodeName` makes pods-on-node lookups O(matches).
- `RolloutReconciler` (`internal/controller/rollout_controller.go`) — opt-in via `--restart-surge-enabled`. Handles restart-surge for Argo Rollouts. Keyed on `Rollout`, watches `Rollout` and `Pod` (mapped via `ResolveWorkloadFromPod` back to its parent Rollout). No node watches; the trigger is the Rollout's own `spec.restartAt`.

Both reconcilers register the same schemes (`corev1`, `rolloutsv1alpha1`) and share leader election; `RecoverOrphans` runs once per reconciler after election.

### Per-workload state machine

Every workload has at most one drain operation at a time, tracked entirely through annotations on the workload object (no CRDs). States, keys, and helpers live in `internal/controller/state.go`:

```
none → pending → scaled-up → ready → draining → done → (annotations cleared)
```

`reconcileWorkload` dispatches on `AnnotationDrainState`. Each handler (`handlePending`, `handleScaleUp`, `handleWaitReady`, `handleWaitEviction`, `handleScaleDown`, `handleCleanup`) advances the state, then patches the workload and returns `RequeueAfter: cfg.RequeueInterval`. Handlers are idempotent so a crashed/restarted controller resumes correctly from whatever annotation is on disk.

Two safety mechanisms are layered on top of the state machine:
- **Stale/timeout abort** — at the top of every reconcile, if `AnnotationDrainStart` is older than `ReadinessTimeout` (or 3× for "stale"), the drain is aborted and replicas restored.
- **Competing-controller re-apply** — `handleScaleUp` and `handleWaitReady` detect external resets of `spec.replicas` (e.g. ArgoCD reconciling to git) and re-apply the surge. Document this to ArgoCD/Flux users as `ignoreDifferences` on `spec.replicas`.

### Restart-surge state machine

Disabled by default. Enabled with `--restart-surge-enabled`. Lives in `rollout_controller.go` (reconciler) and `restart_detect.go` (trigger logic). Only applies to Argo Rollouts — Deployments don't need it because `kubectl rollout restart deployment/...` changes `spec.template` and the new ReplicaSet replaces pods via direct delete (not eviction), so PDBs are not consulted.

The trigger problem: Argo Rollouts' `PodRestarter` (in `rollout/restart.go` of the argo-rollouts repo) goes via the eviction API and retries every 30 seconds **without** emitting Kubernetes Events, status Conditions, or `status.message` updates on PDB rejection. There is no first-class "stuck" signal in the Rollout API. We derive it from:

- `spec.restartAt != nil`
- `status.restartedAt != spec.restartAt` (or unset)
- `now >= spec.restartAt + RestartSurgeGracePeriod` (default 60s — lets Argo complete the restart on its own when PDB permits)
- at least one selected pod with `creationTimestamp < spec.restartAt` and `deletionTimestamp == nil`

States, parallel to drain but stored under disjoint annotation keys (`AnnotationRestartSurgeState`, `AnnotationRestartSurgeStart`):

```
none → pending → scaled-up → ready → draining → done → (cleared)
```

Handler responsibilities:
- `handleRestartPending` — runs the `isRestartStuck` detector, gates on opt-in/single-replica/stable/PDB exists/HPA-maxReplicas-sufficient. On accept, scales 1→2 (or patches HPA `minReplicas`) and stamps annotations.
- `handleRestartWaitReady` — waits until at least 2 selector-matched, non-terminating pods are Ready. Counts pods directly rather than checking `node != drainNode` (no node concept here).
- `handleRestartWaitForArgo` — waits for Argo to finish: either `status.restartedAt == spec.restartAt`, or no remaining pods predate `spec.restartAt`. On completion, restores HPA and scales back to original.
- `handleRestartScaleDown` / `handleRestartCleanup` — same shape as drain.

Safety:
- Stale/timeout abort (`RestartSurgeTimeout`, default 10m; 3× for "stale") at the top of every reconcile.
- Competing-controller re-apply in `handleRestartScaleUp` and `handleRestartWaitReady`, identical to drain.
- **Drain-takes-over yield**: if a drain operation starts on a workload mid-restart-surge, `yieldToDrain` restores replicas to original and clears only the restart-surge **exclusive** annotations (state/start/restartAt), preserving shared keys (`AnnotationOriginalReplicas`, HPA keys) for the drain controller. Use `clearRestartSurgeExclusiveAnnotations` for this — `clearRestartSurgeAnnotations` is for a full cycle and also clears the shared keys.

Orphan recovery on leader election: any Rollout with `AnnotationRestartSurgeState` whose `spec.restartAt` is cleared or whose `status.restartedAt` has caught up is aborted (scale back, clear).

### HPA-aware scaling

When an HPA targets the workload, the controller never touches `spec.replicas` directly — the HPA always wins. Instead `handlePending` patches the HPA's `spec.minReplicas` to `original+1` (saving the original in `AnnotationHPAOriginalMinReplicas` and the HPA name in `AnnotationHPAName`), and `handleWaitEviction` / `abortWorkload` call `restoreHPA` to put it back. HPAs with `maxReplicas <= 1` or `maxReplicas < original+1` cause the workload to be skipped (recorded as a `DrainSkipped` event). See `scaler.go::FindMatchingHPA` / `PatchHPAMinReplicas`. The restart-surge state machine reuses the same HPA helpers and shared annotations.

### Workload abstraction

`internal/controller/workload.go` defines `DrainableWorkload`, implemented by `RolloutWorkload` and `DeploymentWorkload`. Differences worth knowing:
- `IsStable` — Rollouts: `status.phase == Healthy`; Deployments: a `Progressing=NewReplicaSetAvailable` condition, or `observedGeneration == generation` as fallback.
- `CanSurge` — Rollouts: always true; Deployments: false if strategy is `Recreate`.
- `Patch` — uses a JSON merge patch that sets `spec.replicas` and any present drain annotations, while explicitly setting absent drain annotation keys to `null` so they are deleted server-side. This is how `clearDrainAnnotations` → `Patch` deletes annotations in a single round trip.

To add a new workload kind: implement the interface, plumb it into `ResolveWorkloadFromPod` (which walks `Pod → ReplicaSet → owner`) and `findWorkloadsWithDrainNode`, and add it as a `Watches(...)` source in `SetupWithManager`.

### PDB matching

A workload is only processed if a PDB selects (a superset of) its pods. `FindMatchingPDB` requires every PDB selector requirement to also appear in the workload's pod selector — this means PDBs with broader selectors (covering more pods than the workload) match, while PDBs with extra requirements not present on workload pods do not. The PDB is the actual mechanism that blocks Karpenter from evicting before the surge is ready; without one the surge would be pointless, so we refuse to act.

### Orphan recovery

On leader election (in `main.go`), `RecoverOrphans` lists every Rollout and Deployment, and for any with an `AnnotationDrainNode` that points to a node which is no longer tainted (or no longer exists), it aborts and restores. This handles controller crashes mid-operation and node deletions that arrived before the controller could observe them.

### Configuration

`internal/config/config.go`. All config is CLI flags (no env vars, no config file). Defaults:
- `--enabled-annotation=k8s-drain-surge.io/enabled`
- `--requeue-interval=5s`
- `--readiness-timeout=10m`
- `--leader-elect=true`
- `--metrics-addr=:8080` / `--health-addr=:8081`
- `--restart-surge-enabled=false` (opt-in)
- `--restart-surge-grace-period=60s` — wait this long after `spec.restartAt` before triggering a surge, to let Argo finish the restart on its own when the PDB permits.
- `--restart-surge-timeout=10m` — total budget for one restart-surge operation; abort and restore on overrun.
- Drain taints (hardcoded in `DefaultDrainTaints`, not configurable via flag): `karpenter.sh/disrupted`, `ToBeDeletedByClusterAutoscaler`, `node.kubernetes.io/unschedulable`. If you need to add a taint, edit `config.go`.

`Validate` enforces `ReadinessTimeout > RequeueInterval`. When `RestartSurgeEnabled=true`, additionally enforces `RestartSurgeTimeout > RestartSurgeGracePeriod > RequeueInterval`.

## Layout

- `cmd/controller/` — `main.go` only
- `internal/config/` — flag parsing + validation
- `internal/controller/` — all reconciler logic; tests live alongside (`*_test.go`)
- `deploy/helm/k8s-drain-surge/` — chart; values map 1:1 onto controller flags
- `hack/test-*.yaml` — sample workloads (Deployment, Rollout canary, Rollout blue-green) for manual cluster testing
- `docs/specs/plan-k8s-drain-surge.md` — design notes

## Conventions

- Annotation keys are prefixed `k8s-drain-surge.io/` and centralized in `state.go`. Add new ones to both the const block and `drainAnnotationKeys` so bulk cleanup picks them up.
- Every state transition that changes replicas or annotations goes through `wl.Patch(...)`, which uses a `MergePatchType`. Do not use the cached object to `Update` — the merge-patch path is what makes annotation deletion atomic.
- Emit a Kubernetes `Event` (via `r.Recorder`) at every user-visible decision: skip reasons (`DrainSkipped`, `NoPDB`), scale up/down (`DrainSurge`, `DrainScaleDown`), completion (`DrainComplete`), and aborts (`DrainAborted`, `DrainTimeout`, `DrainStale`, `CompetingController`). Operators rely on these for debugging.
- Log lines use structured `logger.WithValues(...)`. Keep them at `Info` for state transitions and `V(1)` for "still waiting" polling messages so the default log volume stays sane.
