# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A Kubernetes controller (Go, controller-runtime) that prevents downtime for single-replica workloads during disruptive eviction events. Three protections:

1. **Drain-surge** (always on) — when a node gets a drain taint, scale opted-in `Deployment`s and Argo `Rollout`s from 1→2 replicas, wait for the surge pod Ready on a different node, let the eviction happen, scale back.
2. **Restart-surge** (opt-in via `--restart-surge-enabled`) — when an Argo Rollout's `spec.restartAt` is blocked by a PDB (Argo's `PodRestarter` uses the eviction API and a single-replica PDB rejects it indefinitely), surge 1→2 so the PDB permits the eviction, then scale back once Argo finishes.
3. **Karpenter-surge** (default ON, `--karpenter-surge-enabled=true`) — when Karpenter's *disruption* controller refuses to taint a node because a PDB would block eviction in dry-run (`disruptionsAllowed=0`), surge 1→2 so the PDB allows consolidation. The trigger is the PDB itself, not a Karpenter Event. Default on because the PDB this controller asks operators to create is precisely what causes the Karpenter pre-taint stuck case — opting users in to the workaround would transfer to them a problem the controller's own prerequisite introduces. See `docs/specs/plan-karpenter-pretaint-surge.md` for the upstream code paths that lead to this gap.

Module: `github.com/aeyrtonvs/k8s-drain-surge`. Go 1.25. Builds against `k8s.io/*` v0.30 and `controller-runtime` v0.18.

## Dev environment

The repo ships with a VS Code devcontainer (`.devcontainer/`) so the toolchain (Go 1.25, make) is reproducible and CI-equivalent locally.

- **Image**: `golang:1.25-bookworm`. Go is preinstalled at `/usr/local/go/bin` and on `PATH` by default.
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
- `make docker-build` — single-arch buildx image loaded into the local daemon (host arch only — `--load` can't emit a manifest list). Override with `IMG=` / `TAG=`.
- `make docker-push` — multi-arch buildx build (`linux/amd64,linux/arm64` by default; override with `PLATFORMS=`) pushed directly to the registry. Release CI normally handles this — see `.github/workflows/release.yaml`. Requires `docker buildx create --use` once to set up a container-driver builder.
- `make helm-package` — chart tarball to `bin/`; `helm lint deploy/helm/k8s-drain-surge` is run in CI
- `make helm-push` — `helm-package` then push to `oci://ghcr.io/aeyrtonvs/charts`

CI (`.github/workflows/ci.yaml`) runs: `go mod tidy`, `go vet`, `go test -race`, build, docker build (no push), `helm lint`. Match these locally before pushing.

## Architecture

### Entry point and reconciler topology

`cmd/controller/main.go` wires a controller-runtime manager with up to three reconcilers:

- `NodeReconciler` (`internal/controller/node_controller.go`) — always on. Handles node drains. Keyed on `Node`, watches `Node`, `Pod` (mapped to `spec.nodeName`), and `Rollout`/`Deployment` (mapped to their `AnnotationDrainNode`). A field index on `Pod.spec.nodeName` makes pods-on-node lookups O(matches).
- `RolloutReconciler` (`internal/controller/rollout_controller.go`) — opt-in via `--restart-surge-enabled`. Handles restart-surge for Argo Rollouts. Keyed on `Rollout`, watches `Rollout` and `Pod` (mapped via `ResolveWorkloadFromPod` back to its parent Rollout). No node watches; the trigger is the Rollout's own `spec.restartAt`.
- `KarpenterSurgeReconciler` (`internal/controller/karpenter_controller.go`) — opt-in via `--karpenter-surge-enabled`. Handles the Karpenter pre-taint surge for both Rollouts and Deployments. Keyed on the workload, watches `Rollout`, `Deployment`, `Pod`, and `PodDisruptionBudget`. Trigger: PDB transitions to `disruptionsAllowed=0` (via watch predicate) or the absolute backup scanner fires (covers informer desync).

All reconcilers register the same schemes (`corev1`, `rolloutsv1alpha1`) and share leader election; `RecoverOrphans` runs once per reconciler after election.

### Per-workload state machine

Every workload has at most one drain operation at a time, tracked entirely through annotations on the workload object (no CRDs). States, keys, and helpers live in `internal/controller/state.go`:

```
none → pending → scaled-up → ready → draining → done → (annotations cleared)
```

`reconcileWorkload` dispatches on `AnnotationDrainState`. Each handler (`handlePending`, `handleScaleUp`, `handleWaitReady`, `handleWaitEviction`, `handleScaleDown`, `handleCleanup`) advances the state, then patches the workload and returns `RequeueAfter: cfg.RequeueInterval`. Handlers are idempotent so a crashed/restarted controller resumes correctly from whatever annotation is on disk.

Two safety mechanisms are layered on top of the state machine:
- **Stale/timeout abort** — at the top of every reconcile, if `AnnotationDrainStart` is older than `ReadinessTimeout` (or 3× for "stale"), the drain is aborted and replicas restored.
- **Competing-controller re-apply** — `handleScaleUp` and `handleWaitReady` detect external resets of `spec.replicas` (e.g. ArgoCD reconciling to git) and re-apply the surge. The ArgoCD `ignoreDifferences` snippet that users should add is already documented in `README.md` §"ArgoCD / FluxCD users" — point operators there rather than re-deriving it.

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

### Karpenter pre-taint surge state machine

**Enabled by default** (`--karpenter-surge-enabled=true`). Lives in `karpenter_controller.go` (reconciler), `karpenter_detect.go` (PDB-state helpers + grace-period tracker), and `karpenter_pdb_predicate.go` (watch filter + PDB→workload map). Applies to both Rollouts and Deployments (the trigger is the PDB, not a workload-kind-specific signal). The default is on because the install guide already asks operators to create a `minAvailable: 1` PDB, and that very PDB is what triggers the Karpenter pre-taint stuck case — making this opt-in would hand operators a regression they did not cause.

The trigger problem: Karpenter's disruption controller (`pkg/controllers/disruption/controller.go::disrupt`, validated by `ValidatePodsDisruptable`) dry-runs the eviction and, when a PDB would reject it, **drops the candidate before applying `karpenter.sh/disrupted`**. Consolidation then retries every 10s (`pollingPeriod`) emitting `DisruptionBlocked … Pdb prevents pod evictions` Events without ever tainting. Result: drain-surge's taint-driven path never fires. The termination controller (manual delete, expiration, spot-interrupt) does apply the taint and is still covered by `NodeReconciler`.

Trigger is the PDB itself:
- **Watch predicate**: `Create` with `disruptionsAllowed == 0`, or `Update` with `old.disruptionsAllowed > 0 && new == 0`. Other transitions (`0→1`, `0→0`, deletes) are rejected — the reconciler picks up unblocks via its own polling.
- **Backup scanner**: an absolute ticker every `--karpenter-surge-scan-period` (default 60s) lists PDBs cluster-wide and emits synthetic enqueue events for any with `disruptionsAllowed == 0`. Covers informer cache desync after crash/restart.

States, stored under disjoint annotation keys (`AnnotationKarpenterSurgeState`, `AnnotationKarpenterSurgeStart`, `AnnotationKarpenterSurgePDB`):

```
none → pending → scaled-up → ready → draining → done → (cleared)
```

Handler responsibilities:
- `handleKarpenterPending` — runs the gate suite (opt-in → no other state machine active → stable → single-replica → CanSurge → PDB found and blocked → `pdbWouldAllowSurge(pdb, replicas+1)` → workload pod's node has no drain taint → HPA permits surge → grace period elapsed). Tracks first-observation timestamps per `pdb.UID` in an in-memory `gracePeriodTracker` (rebuilt on restart; worst case: one extra grace period of delay). On accept, surges 1→2 (or patches HPA `minReplicas`) and stamps annotations.
- `handleKarpenterScaleUp` / `handleKarpenterWaitReady` — same competing-controller re-apply pattern as drain.
- `handleKarpenterReady` — waits for one of three transitions: a drain taint appears on a workload pod's node (yield via `yieldToDrain`), the PDB unblocks (transition to `draining`), or timeout (`abortKarpenterSurge`).
- `handleKarpenterScaleDown` — enforces the **R9 invariant** (only undo what we applied): if `spec.replicas != original+1` (no HPA) or `hpa.spec.minReplicas != original+1` (HPA), the operator/ArgoCD changed things externally; emit `KarpenterSurgeYielded` with reason `ExternalScaleChange` and clear annotations **without** touching replicas. Otherwise restore HPA / scale back to original.
- `handleKarpenterCleanup` — clears annotations, emits `KarpenterSurgeComplete`.

Safety:
- Stale/timeout abort (`KarpenterSurgeTimeout`, default 10m; 3× for "stale") at the top of every reconcile.
- **Drain-takes-over yield**: a drain real that lands on a workload mid-karpenter-surge wins. `yieldToDrain` clears only the karpenter-surge **exclusive** keys (state/start/PDB) and preserves shared bookkeeping (`AnnotationOriginalReplicas`, HPA keys) so the drain machinery can finish the cycle. Use `clearKarpenterSurgeExclusiveAnnotations` for that path; `clearKarpenterSurgeAnnotations` is full-cycle cleanup.
- **R5 not handled in MVP**: when Karpenter consolidates the *surge* pod instead of the original (it sees `disruptionsAllowed=1` and picks any candidate), timeout fires without resolving the bottleneck. Workaround documented in README: `kubectl cordon <original-node>`.

Orphan recovery on leader election: any workload with `AnnotationKarpenterSurgeState` whose PDB no longer exists, has `disruptionsAllowed >= 1`, or whose surge started >3× timeout ago is aborted (restore + clear). Also aborts every active surge when the feature gate is turned off.

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
- `--karpenter-surge-enabled=true` (default on — see rationale in the "Karpenter pre-taint surge state machine" section)
- `--karpenter-surge-grace-period=60s` — time a PDB must stay at `disruptionsAllowed=0` before triggering a karpenter-surge (≈ 6 Karpenter pollingPeriod cycles; discards transient blocks).
- `--karpenter-surge-timeout=10m` — total budget for one karpenter-surge operation; abort and restore on overrun.
- `--karpenter-surge-scan-period=60s` — absolute backup ticker that scans PDBs cluster-wide for stuck workloads (covers informer desync after crash/restart). Must be `>= --requeue-interval`.
- Drain taints (hardcoded in `DefaultDrainTaints`, not configurable via flag): `karpenter.sh/disrupted`, `ToBeDeletedByClusterAutoscaler`, `node.kubernetes.io/unschedulable`. If you need to add a taint, edit `config.go`. Note: the `karpenter.sh/disrupted` taint is **only** applied by Karpenter's termination controller (manual delete, expiration, spot-interrupt). The disruption controller refuses to apply it when a PDB would block eviction — that gap is what karpenter-surge addresses.

`Validate` enforces `ReadinessTimeout > RequeueInterval`. When `RestartSurgeEnabled=true`, additionally enforces `RestartSurgeTimeout > RestartSurgeGracePeriod > RequeueInterval`. When `KarpenterSurgeEnabled=true`, enforces `KarpenterSurgeTimeout > KarpenterSurgeGracePeriod` and `KarpenterSurgeScanPeriod >= RequeueInterval`.

## Layout

- `cmd/controller/` — `main.go` only
- `internal/config/` — flag parsing + validation
- `internal/controller/` — all reconciler logic; tests live alongside (`*_test.go`). `logfields.go` centralizes structured-log key constants (`LogFieldWorkload`, `LogFieldDrainNode`, …) and the cache field-index keys (`IndexPodNodeName`, `IndexWorkloadDrainNode`) — reuse these constants instead of hardcoding strings so log vocabulary stays stable for downstream consumers (Loki, Datadog).
- `deploy/helm/k8s-drain-surge/` — chart; values map 1:1 onto controller flags
- `hack/test-*.yaml` — sample workloads (Deployment, Rollout canary, Rollout blue-green) for manual cluster testing
- `docs/specs/plan-k8s-drain-surge.md` — design notes

## Conventions

- Annotation keys are prefixed `k8s-drain-surge.io/` and centralized in `state.go`. Add new ones to both the const block and `drainAnnotationKeys` so bulk cleanup picks them up.
- Every state transition that changes replicas or annotations goes through `wl.Patch(...)`, which uses a `MergePatchType`. Do not use the cached object to `Update` — the merge-patch path is what makes annotation deletion atomic.
- Emit a Kubernetes `Event` (via `r.Recorder`) at every user-visible decision: skip reasons (`DrainSkipped`, `NoPDB`), scale up/down (`DrainSurge`, `DrainScaleDown`), completion (`DrainComplete`), and aborts (`DrainAborted`, `DrainTimeout`, `DrainStale`, `CompetingController`). Operators rely on these for debugging.
- Log lines use structured `logger.WithValues(...)` with the keys defined in `logfields.go`. Keep them at `Info` for state transitions and `V(1)` for "still waiting" polling messages so the default log volume stays sane.

## Troubleshooting / operator-facing docs

User-facing symptom → cause → fix tables live in `README.md` §Troubleshooting (skip reasons, HPA `maxReplicas=1`, stuck replicas, Windows pod timeouts, the "extra pod during scale-down" expected-behavior note, manual cleanup commands). When a user reports a symptom, check that section first and update it there rather than duplicating troubleshooting prose here — CLAUDE.md is for architecture, README.md is the single source of truth for operator-facing remediation.
