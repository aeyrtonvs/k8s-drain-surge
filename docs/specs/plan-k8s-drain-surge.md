# Plan: k8s-drain-surge

## Context

Cuando Karpenter consolida nodos, usa la Eviction API para drenar pods. Para workloads con 1 replica (Argo Rollouts o Deployments) esto causa downtime: sin PDB Karpenter evicta directamente, con PDB (`minAvailable:1`) bloquea indefinidamente. El graceful-drain-controller existente solo funciona con Deployments y usa restart annotation (que en Argo Rollouts mata el pod estable inmediatamente). Este controller hace scale-up temporal (1→2) antes de permitir la eviction, logrando zero-downtime para ambos tipos de workload.

**Proyecto nuevo** — repo público en GitHub, imagen en ghcr.io. Soporta Argo Rollouts (Canary y Blue-Green) y Kubernetes Deployments.

---

## Decisiones de diseño

- **Detección**: Watch de Node taints via informer (no webhook)
- **Surge**: Scale up temporal replicas 1→2, no restart annotation
- **Lenguaje**: Go + controller-runtime
- **Scope**: Opt-in via annotation `k8s-drain-surge.io/enabled: "true"`
- **Workloads soportados**: Argo Rollouts (Canary, Blue-Green) y Deployments

---

## Soporte dual: Argo Rollouts + Deployments

### Ownership resolution

Al recorrer `ownerReferences` del ReplicaSet, el controller determina el tipo de workload:

| Owner kind | apiVersion | Tipo |
|---|---|---|
| `Rollout` | `argoproj.io/v1alpha1` | Argo Rollout |
| `Deployment` | `apps/v1` | Deployment |

La lógica del state machine es idéntica para ambos. Las diferencias están encapsuladas en el paso de resolución y validación:

| Aspecto | Argo Rollout | Deployment |
|---|---|---|
| Ownership chain | Pod → RS → Rollout | Pod → RS → Deployment |
| Phase check | `status.phase == Healthy` | No tiene condition equivalente conflictiva — verificar que no esté mid-rollout via `status.conditions` |
| Scale patch | `rollout.spec.replicas` | `deployment.spec.replicas` |
| Strategy validation | No requerida (scale-up no dispara progression) | Verificar `strategy.type == RollingUpdate` con `maxUnavailable: 0` — si no, el Deployment podría matar el pod viejo durante scale-down antes de que el nuevo esté ready |
| ArgoCD concern | `ignoreDifferences` para `/spec/replicas` | Mismo concern si ArgoCD gestiona el Deployment |

### Implementación

Un interface `DrainableWorkload` abstrae las diferencias:

```go
type DrainableWorkload interface {
    GetReplicas() int32
    SetReplicas(int32)
    IsStable() bool              // Healthy para Rollout, not-progressing para Deployment
    GetPodSelector() labels.Selector
    GetObjectMeta() metav1.ObjectMeta
    Patch(ctx, client) error
}
```

Dos implementaciones: `RolloutWorkload` y `DeploymentWorkload`. El reconciler trabaja con la interface — el state machine no sabe qué tipo de workload está manejando.

---

## Riesgos identificados y mitigaciones

### R1: Scale-up en Canary vs Blue-Green (BAJO)

Patchar `spec.replicas` en un Rollout estable solo escala el stable/active ReplicaSet. No dispara canary steps ni blue-green promotion porque solo `spec.template` triggers progression. Confirmado por soporte de HPA en Argo Rollouts.

**Mitigación**: Verificar `status.phase == Healthy` antes de patchar. Si el Rollout está `Progressing` o `Paused` (mid-rollout), skip con log.

### R2: Race condition Karpenter eviction vs scale-up (MEDIO)

El PDB es evaluado por el API server en cada intento de eviction. Mientras haya solo 1 pod Ready y `minAvailable:1`, toda eviction retorna 429. El pod nuevo Pending/not-Ready no cuenta para el PDB. La eviction queda bloqueada hasta que el pod nuevo pase readiness.

**Mitigaciones**:
- PDB debe existir ANTES de que el controller actúe — el controller verifica que existe un PDB con selector matching, si no existe: skip + warning
- Si el NodePool tiene `terminationGracePeriod` configurado, debe ser **mayor** que el `--readiness-timeout` del controller. Si Karpenter force-termina el nodo antes de que el surge complete, bypasea PDBs y causa downtime. Sin `terminationGracePeriod`, Karpenter espera indefinidamente (comportamiento deseado). Nota: esto es el `terminationGracePeriod` del **NodePool de Karpenter**, no el `terminationGracePeriodSeconds` del pod — el del pod es compatible y deseable
- Timeout default 10m (configurable) para cubrir Windows pods que toman 5-7 min en estar Ready

### R3: Argo Rollouts controller / ArgoCD peleando por replicas (BAJO sin ArgoCD, ALTO con ArgoCD)

Argo Rollouts controller no resetea replicas — lee `spec.replicas` del objeto y escala el RS. Pero **ArgoCD sí**: si el manifest en Git dice `replicas: 1` y auto-sync está activo, detecta drift y resetea a 1. Mismo concern aplica para Deployments gestionados por ArgoCD o FluxCD.

**Mitigaciones**:
- Documentar que si ArgoCD gestiona el workload, requiere `ignoreDifferences` para `/spec/replicas` con `RespectIgnoreDifferences` en syncOptions
- El controller guarda `original-replicas` en annotation para no asumir que siempre era 1
- Si en la siguiente reconciliación replicas volvió al valor original sin que el controller lo hiciera: log warning "competing controller detected"

### R4: Pod scheduling durante consolidación (ALTO)

El nodo drenado tiene taint NoSchedule. El pod nuevo necesita otro nodo. Si no hay capacidad, Karpenter debe provisionar uno nuevo. Karpenter SÍ provisiona nodos para pods Pending incluso durante consolidación (loops separados). Pero en Windows el tiempo nodo+pod puede ser 12-17 minutos.

**Mitigaciones**:
- Timeout configurable, default 10m, documentar que para Windows puede necesitar 15-20m
- El controller no hace pre-check de capacidad (complejidad excesiva) — confía en Karpenter para provisionar
- Documentar la recomendación de warm pool / slight over-provisioning en NodePools Windows

### R5: Blue-Green traffic routing durante 2 replicas (BAJO)

El pod nuevo se crea en el mismo ReplicaSet (mismo `rollouts-pod-template-hash`). El active service lo incluye automáticamente. Ambos pods reciben tráfico durante la ventana temporal.

**Mitigación**: Solo documentar. Si la app tiene concerns de estado (session affinity, cache in-memory), el usuario debe estar consciente.

### R6: Orphan state — controller crash mid-operación (ALTO)

Si el controller muere entre SCALED_UP y scale-back, el workload queda en 2 replicas permanentemente.

**Mitigaciones**:
- Estado persistido en annotations del workload (no in-memory) — state machine es reentrant
- On startup: scan de Rollouts y Deployments con annotation `drain-state` que no corresponden a un nodo tainted → force abort
- Hard timeout: si `drain-start` > 3x readinessTimeout → force scale-back sin importar el estado
- Correr en nodos sistema (managed node group) que Karpenter no gestiona
- PriorityClass `system-cluster-critical`
- Leader election para HA con 2 replicas

### R7: Deployment strategy incompatible (MEDIO)

Si un Deployment tiene `strategy.type: Recreate` o `maxUnavailable > 0`, durante el scale-down de 2→1 el Deployment controller podría matar el pod viejo antes de que el nuevo esté ready.

**Mitigación**: Para Deployments, verificar que `strategy.type == RollingUpdate`. No es necesario validar `maxSurge`/`maxUnavailable` porque el controller no hace rollout — solo cambia `spec.replicas`. El Deployment controller simplemente reduce el ReplicaSet de 2 a 1, terminando un pod. Como en ese punto el pod viejo ya fue evictado por Karpenter, solo queda el pod nuevo que ya está ready. Sin embargo, si `strategy.type == Recreate`, skip con warning por seguridad.

---

## Arquitectura del controller

### State machine (idéntica para Rollouts y Deployments)

```
Node tainted (karpenter.sh/disrupted)
  → PENDING: verificar PDB existe, escribir annotations, patch replicas 1→2
  → SCALED_UP: poll hasta que nuevo pod esté Ready en otro nodo
  → READY: esperar que Karpenter evicte pod viejo (PDB satisfecho)
  → DRAINING: patch replicas 2→1
  → DONE: limpiar annotations
```

Abort paths: timeout, taint removido, nodo eliminado, no PDB encontrado.

### Watches (4 fuentes → reconcile por Node)

1. **Node** (primary): detecta taints
2. **Pod** (secondary): mapea `pod.spec.nodeName` → Node reconcile
3. **Rollout** (secondary): mapea annotation `drain-node` → Node reconcile
4. **Deployment** (secondary): mapea annotation `drain-node` → Node reconcile

### Annotations en el workload (Rollout o Deployment)

| Key | Valor | Set by |
|---|---|---|
| `k8s-drain-surge.io/enabled` | `"true"` | Usuario |
| `k8s-drain-surge.io/drain-state` | `pending\|scaled-up\|ready\|draining\|done` | Controller |
| `k8s-drain-surge.io/original-replicas` | `"1"` | Controller |
| `k8s-drain-surge.io/drain-node` | nombre del nodo | Controller |
| `k8s-drain-surge.io/drain-start` | timestamp RFC3339 | Controller |

### Filtros antes de actuar

- Annotation `enabled: "true"` presente
- `spec.replicas == 1` (o nil) — o drain ya en progreso
- Workload estable: `status.phase == Healthy` (Rollout) o not progressing (Deployment)
- No tiene `drain-node` apuntando a OTRO nodo
- Existe un PDB con selector que match los pods del workload
- Si hay HPA: solo skip si `maxReplicas == 1` (no permite surge). Si `maxReplicas > 1`: proceder — el `stabilizationWindowSeconds` del HPA (default 300s para scale-down) protege el surge temporal
- Para Deployments: `strategy.type == RollingUpdate` (skip si Recreate)

### Scaling

MergePatch sobre `spec.replicas` del workload. Para Rollouts, solo escala el stable ReplicaSet sin disparar progression. Para Deployments, escala el único ReplicaSet activo.

---

## Estructura del proyecto

```
k8s-drain-surge/
├── cmd/controller/main.go              # Entry point, scheme registration, manager setup
├── internal/
│   ├── controller/
│   │   ├── node_controller.go          # Reconciler + state machine
│   │   ├── node_controller_test.go
│   │   ├── workload.go                 # DrainableWorkload interface + implementations
│   │   ├── workload_test.go
│   │   ├── scaler.go                   # Scale, ownership resolution, pod lookup, PDB check
│   │   ├── scaler_test.go
│   │   └── state.go                    # State types, annotation constants, transitions
│   └── config/config.go                # CLI flags + env var config
├── deploy/helm/k8s-drain-surge/
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
│       ├── _helpers.tpl
│       ├── deployment.yaml
│       ├── serviceaccount.yaml
│       ├── clusterrole.yaml
│       └── clusterrolebinding.yaml
├── hack/
│   ├── test-rollout-canary.yaml        # Sample Canary Rollout + PDB
│   ├── test-rollout-bluegreen.yaml     # Sample Blue-Green Rollout + PDB
│   ├── test-deployment.yaml            # Sample Deployment + PDB
│   └── test-manual.sh                  # Script de pruebas manual
├── Dockerfile
├── Makefile
├── go.mod
└── README.md
```

### Dependencias

- `sigs.k8s.io/controller-runtime` v0.18.x
- `github.com/argoproj/argo-rollouts` v1.7.x
- `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` v0.30.x

---

## Reconciliation detallado

### Reconcile(ctx, Request{Name: nodeName})

1. **GET Node** — si NotFound: listar workloads con `drain-node == nodeName`, transicionar a DRAINING
2. **Check taints** — si no tiene taint de drain: abort todos los workloads asociados
3. **List pods** en el nodo (excluir Succeeded/Failed/deleting)
4. **Resolver workloads** — Pod → RS → Rollout o Deployment via ownerReferences, deduplicar
5. **Filtrar** — opt-in, replicas==1, estable, PDB existe, HPA compatible, strategy compatible (Deployments)
6. **State machine por workload**:
   - `""` → verificar PDB + escribir annotations + scale up → `scaled-up`, requeue 5s
   - `pending` → safety net: re-apply scale up → `scaled-up`, requeue 5s
   - `scaled-up` → buscar pod Ready en otro nodo. Si existe → `ready`. Si timeout → abort. Sino requeue
   - `ready` → buscar pod viejo en drain-node. Si no existe → scale down → `draining`. Sino requeue
   - `draining` → verificar replicas == original. Si ok → `done`. Sino re-patch
   - `done` → limpiar annotations

### Edge cases

- **Timeout**: configurable (default 10m), abort + scale back + warning event
- **Taint removido**: abort inmediato
- **Controller restart**: estado en annotations, reentrant. Startup scan para orphans (ambos tipos)
- **Rollout mid-deploy**: skip si phase != Healthy
- **Deployment mid-rollout**: skip si progressing condition
- **Pod nuevo en mismo nodo**: check `pod.Spec.NodeName != drainNode`
- **HPA con maxReplicas==1**: skip con warning. Si maxReplicas > 1: compatible
- **Stale state**: hard timeout 3x readinessTimeout, force abort
- **ArgoCD/FluxCD drift**: detectar si replicas fue reseteado externamente, log warning
- **No PDB**: skip + warning event
- **Deployment Recreate strategy**: skip + warning event

---

## RBAC

```yaml
- apiGroups: [""]
  resources: [nodes]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
- apiGroups: [apps]
  resources: [replicasets]
  verbs: [get, list, watch]
- apiGroups: [apps]
  resources: [deployments]
  verbs: [get, list, watch, patch]
- apiGroups: [argoproj.io]
  resources: [rollouts]
  verbs: [get, list, watch, patch]
- apiGroups: [policy]
  resources: [poddisruptionbudgets]
  verbs: [get, list]
- apiGroups: [autoscaling]
  resources: [horizontalpodautoscalers]
  verbs: [get, list]
- apiGroups: [""]
  resources: [events]
  verbs: [create, patch]
- apiGroups: [coordination.k8s.io]
  resources: [leases]
  verbs: [get, create, update]
```

---

## Configuración

| Flag | Default | Descripción |
|---|---|---|
| `--drain-taint` | `karpenter.sh/disrupted`, `ToBeDeletedByClusterAutoscaler`, `node.kubernetes.io/unschedulable` | Taints a detectar |
| `--enabled-annotation` | `k8s-drain-surge.io/enabled` | Annotation opt-in |
| `--requeue-interval` | `5s` | Intervalo de poll |
| `--readiness-timeout` | `10m` | Timeout para pod nuevo |
| `--leader-elect` | `true` | HA con leader election |
| `--metrics-addr` | `:8080` | Prometheus metrics |
| `--health-addr` | `:8081` | Health probes |

---

## Helm chart

### values.yaml (keys principales)

```yaml
replicaCount: 2   # HA con leader election

image:
  repository: ghcr.io/<org>/k8s-drain-surge
  tag: ""         # defaults to appVersion

controller:
  drainTaints:
    - key: "karpenter.sh/disrupted"
      effect: "NoSchedule"
    - key: "ToBeDeletedByClusterAutoscaler"
      effect: "NoSchedule"
    - key: "node.kubernetes.io/unschedulable"
      effect: "NoSchedule"
  readinessTimeout: "10m"
  requeueInterval: "5s"

# Correr en nodos sistema que Karpenter no gestiona
nodeSelector:
  kubernetes.io/os: linux
tolerations:
  - key: CriticalAddonsOnly
    operator: Exists
priorityClassName: system-cluster-critical

resources:
  requests: { cpu: 50m, memory: 64Mi }
  limits: { cpu: 200m, memory: 128Mi }

serviceAccount:
  create: true
```

### Templates incluidos

- `deployment.yaml`: 2 replicas, leader election, liveness/readiness en `:8081`, priorityClass, nodeSelector, tolerations
- `clusterrole.yaml`: RBAC completo (incluye deployments y rollouts)
- `clusterrolebinding.yaml`
- `serviceaccount.yaml`
- `_helpers.tpl`: labels estándar, fullname

---

## Prerequisitos del usuario

1. **Annotation opt-in** en el workload (Rollout o Deployment): `k8s-drain-surge.io/enabled: "true"`
2. **PDB** con `minAvailable: 1` apuntando a los pods del workload
3. **Si usa ArgoCD/FluxCD**: `ignoreDifferences` para `/spec/replicas`
4. **NodePool `terminationGracePeriod`** (Karpenter): si está configurado, debe ser mayor que `--readiness-timeout` del controller. Sin él, Karpenter espera indefinidamente (ideal). Nota: esto NO es el `terminationGracePeriodSeconds` del pod — el del pod es compatible y deseable
5. **Para Windows workloads**: considerar `--readiness-timeout 15m` o más
6. **Para Deployments**: `strategy.type` debe ser `RollingUpdate` (no `Recreate`)

---

## Estrategia de pruebas en cluster real

### Prerequisitos del cluster

- Karpenter instalado y funcionando
- Argo Rollouts controller + CRDs instalados
- kubectl + helm configurados contra el cluster
- Al menos 2 nodos donde puedan correr workloads

### Fixtures de prueba (`hack/`)

**test-rollout-canary.yaml**: Rollout canary con 1 replica, nginx, readiness probe rápido, annotation opt-in, PDB

**test-rollout-bluegreen.yaml**: Rollout blue-green con 1 replica, nginx, annotation opt-in, PDB, active service

**test-deployment.yaml**: Deployment con 1 replica, nginx, strategy RollingUpdate maxSurge:1 maxUnavailable:0, annotation opt-in, PDB

**test-manual.sh**: Script interactivo que guía al operador paso a paso

### Script de pruebas manual (`hack/test-manual.sh`)

```
Fase 1 — Setup
  1. Instalar controller via Helm
  2. Aplicar test-rollout-canary.yaml, test-rollout-bluegreen.yaml, test-deployment.yaml
  3. Verificar pods Running y Ready
  4. Anotar en qué nodo está cada pod

Fase 2 — Test Canary Rollout: taint manual
  5. kubectl taint node <nodo-del-pod-canary> karpenter.sh/disrupted=:NoSchedule
  6. Observar logs del controller (kubectl logs -f)
  7. Verificar: annotation drain-state cambia pending → scaled-up
  8. Verificar: replicas del Rollout = 2, nuevo pod creándose
  9. Esperar: nuevo pod Ready en otro nodo
  10. Verificar: drain-state = ready
  11. Simular eviction: kubectl delete pod <pod-viejo>
  12. Verificar: drain-state = draining → done, replicas vuelve a 1
  13. Verificar: annotations de drain limpiadas
  14. Remover taint: kubectl taint node <nodo> karpenter.sh/disrupted-

Fase 3 — Test Blue-Green Rollout: taint manual
  15-24. Repetir pasos 5-14 con el Rollout blue-green
  25. Verificar que el service activo siempre tuvo endpoints

Fase 4 — Test Deployment: taint manual
  26-35. Repetir pasos 5-14 con el Deployment
  36. Verificar que el service siempre tuvo endpoints

Fase 5 — Test abort por timeout
  37. Aplicar un Rollout con un pod que nunca pasa readiness (imagen fake)
  38. Taintear el nodo
  39. Esperar que pase el timeout
  40. Verificar: controller hace abort, replicas vuelve a 1, warning event emitido

Fase 6 — Test abort por taint removido
  41. Taintear nodo con un workload válido
  42. Esperar drain-state = scaled-up
  43. Remover el taint: kubectl taint node <nodo> karpenter.sh/disrupted-
  44. Verificar: controller hace abort, replicas vuelve a 1

Fase 7 — Test con Karpenter real (si aplica)
  45. Dejar Karpenter consolidar naturalmente (o forzar con bajo utilization)
  46. Observar que el controller intercepta y hace el surge
  47. Verificar zero-downtime con curl loop al service

Fase 8 — Cleanup
  48. Desinstalar fixtures de prueba
  49. Remover taints manuales si quedaron
```

### Validaciones en cada paso

- `kubectl get rollout|deployment <name> -o jsonpath='{.metadata.annotations}'` — verificar annotations
- `kubectl get rollout|deployment <name> -o jsonpath='{.spec.replicas}'` — verificar replica count
- `kubectl get pods -l <selector> -o wide` — verificar pods, nodos, status
- `kubectl get events --field-selector involvedObject.name=<name>` — verificar events del controller
- `kubectl logs -l app.kubernetes.io/name=k8s-drain-surge` — logs del controller

---

## CI/CD (GitHub Actions)

```
on push to main:
  1. go vet + go test
  2. docker buildx build (amd64 only v1, multi-arch futuro)
  3. push a ghcr.io con tag :latest y :sha-<commit>

on tag (v*):
  4. build + push con tag :v1.0.0
  5. helm package + push a ghcr.io OCI registry
```

---

## Secuencia de implementación

1. **Scaffold**: go.mod, main.go, annotations, config, state types
2. **Interface**: workload.go — DrainableWorkload interface con RolloutWorkload y DeploymentWorkload
3. **Core**: node_controller.go (state machine), scaler.go (ownership, scale, PDB check, HPA check)
4. **Tests**: unit tests con envtest — para cada tipo de workload (Rollout canary, Rollout blue-green, Deployment):
   - Happy path
   - No opt-in annotation
   - Timeout
   - Taint removed
   - Controller restart (orphan recovery)
   - No PDB
   - Mid-rollout/progressing
   - HPA maxReplicas==1 (skip)
   - HPA maxReplicas>1 (compatible)
   - Deployment Recreate strategy (skip)
5. **Packaging**: Dockerfile, Helm chart, Makefile
6. **Test fixtures**: hack/test-rollout-canary.yaml, test-rollout-bluegreen.yaml, test-deployment.yaml, test-manual.sh
7. **Docs**: README con prerequisitos, instalación, configuración, troubleshooting
8. **CI**: GitHub Actions workflow
