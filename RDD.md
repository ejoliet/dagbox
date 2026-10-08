# dagbox

> Author, save, trigger, and monitor Airflow DAGs from a local web page. Two backends: **lite** (Docker Compose, LocalExecutor, Postgres, ready in about a minute) and **full** (kind, KubernetesExecutor or CeleryExecutor). Both ship Prometheus and Grafana dashboards on first boot.

RDD type: **A — Spec**. No code exists yet. A coding agent implements from this file. Status: pre-Gate-0.

---

## Purpose

**Problem.** Trying a DAG locally today means a terminal loop: edit a file, wait for the parser, open the Airflow UI, trigger, dig for logs, and set up monitoring by hand. `astro dev start` (Astronomer CLI) shortens this, but it runs Docker Compose, not Kubernetes, so KubernetesExecutor behavior and pod-level metrics are never exercised.

**Solution.** One command stands up a backend, and a static page served on `localhost` gives a browser loop: write or drop a DAG, lint it live with Airflow rules, save it, trigger it, watch task states and logs, manage connections and variables, edit Airflow and dagbox settings with a plan → apply → verify → rollback loop, and open Grafana.

- **Lite (default):** Docker Compose with Postgres, `airflow standalone` (LocalExecutor), statsd-exporter, Prometheus, Grafana.
- **Full:** single-node kind cluster with the official Airflow Helm chart (KubernetesExecutor by default, CeleryExecutor optional) and kube-prometheus-stack.

The page is identical in both modes. It only talks to the Airflow REST API, the DAG folder, and Grafana.

**Who benefits.** Engineers prototyping DAGs before they go to EKS, and new developers learning Airflow on Kubernetes without touching shared infrastructure.

**Why not `astro dev start`.** dagbox runs the real KubernetesExecutor on Kubernetes, ships provisioned Prometheus and Grafana, and has an in-browser editor with live `AIR` lint (ruff compiled to WASM). The editor plus a prod-like executor is the wedge. The run and monitor panels are table stakes.

**Three ways to get a DAG in** (from the original requirement):

| Mode | How |
|------|-----|
| Create | Start from a template in the editor, save to the DAG folder |
| Upload | Drag a `.py` file onto the page; it is linted, then saved to the DAG folder |
| Point | Set `DAGBOX_DAGS_DIR` to an existing DAG checkout before bootstrap |

**Which mode to use:**

| | Lite (default) | Full |
|---|---|---|
| Start time | About 1 min after first image build | About 10 min first run |
| Needs | Docker (4 GB) | Docker (8 GB, 12 GB with Celery), kind, Helm |
| Parallelism | Tasks run as separate processes in one container, capped by `[core] parallelism` | Tasks run as separate pods (or Celery workers) |
| Isolation per task | None: shared CPU, memory, filesystem, Python env | Pod-level resources, `pod_override`, per-task images |
| Grafana | Airflow metrics dashboard | Airflow dashboard + Kubernetes dashboard |
| Connections and variables editing | Yes | Yes |
| Use it for | Writing DAGs, task logic, fan-out and mapping correctness | Executor behavior, pod resources, Kubernetes metrics |

---

## Architecture

### Lite mode

```
 macOS host
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Browser (Chromium)  http://localhost:5173  (same page as full mode)  │
 │  ├─ Save ──writes──► $DAGBOX_DAGS_DIR                                │
 │  ├─ fetch /api/v2/* ──► 127.0.0.1:8080                               │
 │  └─ Grafana ─────────► 127.0.0.1:3000                                │
 │                                                                      │
 │ Docker Compose project "dagbox" (lite/compose.yaml)                  │
 │  postgres        volume dagbox-pg                                    │
 │  airflow         `airflow standalone`, LocalExecutor                 │
 │                  bind mounts: $DAGBOX_DAGS_DIR → /opt/airflow/dags   │
 │                               $DAGBOX_LOGS_DIR → /opt/airflow/logs   │
 │                  StatsD → statsd-exporter:9125                       │
 │  statsd-exporter mapping file shared with full mode                  │
 │  prometheus      scrapes statsd-exporter:9102                        │
 │  grafana         provisioned datasource + dagbox-overview dashboard  │
 │  All published ports bound to 127.0.0.1                              │
 └──────────────────────────────────────────────────────────────────────┘
```

### Full mode

```
 macOS host
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Browser (Chromium)  http://localhost:5173                            │
 │  ├─ Editor: CodeMirror 6 + ruff WASM (F, E9, AIR rules)              │
 │  ├─ Save:  File System Access API ──writes──► $DAGBOX_DAGS_DIR       │
 │  ├─ Run:   fetch /api/v2/* (Bearer JWT) ─────► 127.0.0.1:8080        │
 │  └─ Monitor: iframe / link ──────────────────► 127.0.0.1:3000        │
 │                                                                      │
 │ $DAGBOX_DAGS_DIR ─┐   $DAGBOX_LOGS_DIR ─┐                            │
 │ ┌─────────────────▼─────────────────────▼──────────────────────────┐ │
 │ │ kind node (single)   extraMounts: /dagbox/dags, /dagbox/logs     │ │
 │ │  PV/PVC dags (hostPath, RWO) ─► dag-processor, scheduler,        │ │
 │ │                                 api-server, task pods            │ │
 │ │  PV/PVC logs (hostPath, RWO) ─► task pods write, api-server reads│ │
 │ │  ns airflow:    api-server (NodePort 30080), scheduler,          │ │
 │ │                 dag-processor, triggerer, postgres, statsd       │ │
 │ │                 [+ redis, celery workers with --celery]          │ │
 │ │  ns monitoring: prometheus (NodePort 30090), grafana (30300)     │ │
 │ └──────────────────────────────────────────────────────────────────┘ │
 └──────────────────────────────────────────────────────────────────────┘
```

**Data flow**

- Editor → File System Access API → `$DAGBOX_DAGS_DIR` → (lite: bind mount | full: kind node mount → DAG PVC) → LocalDagBundle → dag-processor → Postgres.
- Page → `POST /api/v2/dags/{id}/dagRuns` → scheduler → executor → task process (lite) or task pod (full) → logs folder → `GET .../logs/{try}` → page.
- Page → `/api/v2/connections`, `/api/v2/variables` → Postgres (secrets encrypted with the Fernet key).
- Airflow StatsD → statsd-exporter (shared mapping file) → Prometheus (lite: static scrape | full: ServiceMonitor) → Grafana.

**Key components**

| Component | Responsibility |
|-----------|----------------|
| `web/` | Static page: editor, lint, save, run, logs, admin, Grafana panel |
| `scripts/bootstrap.sh` | Idempotent install for either mode: tools check, image build, lite Compose up or full cluster + Helm, self-test, writes `.env.local` |
| `lite/compose.yaml` | Lite backend: postgres, airflow, statsd-exporter, prometheus, grafana |
| `monitoring/statsd-mapping.yaml` | One StatsD → Prometheus mapping used by both modes, so dashboards work unchanged |
| `scripts/apply.sh` | Config lifecycle: validate overrides, plan (diff), snapshot, apply (compose or Helm), self-test, write result file |
| `config/keys.json` | Single source for setting classes (`restart`, `recreate`, `migration`, `locked`), read by page and scripts |
| `$DAGBOX_HOME/dagbox.overrides.json` | User-edited settings, written by the page, read by `make apply`; never holds secrets |
| `scripts/status.sh` | Health of cluster, pods, endpoints, login self-test |
| `scripts/teardown.sh` | Lite: `docker compose down` (keeps Postgres volume unless `--purge`). Full: delete cluster. Host DAG and log folders always kept |
| `cluster/kind.yaml.tmpl` | Node config: mounts and port mappings bound to `127.0.0.1` |
| `cluster/airflow-values.yaml` | Airflow Helm values |
| `cluster/monitoring-values.yaml` | kube-prometheus-stack values |
| `cluster/manifests/` | PVs/PVCs, auth Secret, ServiceMonitor, dashboard ConfigMap |
| `image/Dockerfile` | `apache/airflow:<version>` + `requirements.txt` |

> **Rationale.** The browser never talks to Kubernetes. It talks only to the Airflow REST API and the local filesystem. This keeps the page static, keeps credentials out of the browser except for a short-lived JWT, and makes the backend swappable later (Git or S3 DAG bundles) without changing the page's run and monitor code.

---

## Recommended Stack

Decisions are based on official docs checked on 2026-10-06/07. GitHub star and download counts were not collected; every choice below is a mainstream, maintained default for its layer.

| Layer | Chosen | Why | Rejected |
|-------|--------|-----|----------|
| Orchestrator | Apache Airflow **3.3.2** | Current line; 3.1.x is past maintenance | 3.1.8 (EOL; available via `DAGBOX_AIRFLOW_VERSION` for prod parity) |
| Airflow install | Official Apache Airflow Helm chart | Same chart family as EKS deployments; has `dags.persistence` and `logs.persistence` | Astronomer chart, Compose |
| Lite backend | Docker Compose: `airflow standalone` + Postgres + statsd-exporter + Prometheus + Grafana | One Airflow container, fewest moving parts; Postgres avoids SQLite concurrency limits with parallel tasks | Split api-server/scheduler/dag-processor containers (fallback, see Q9), SQLite |
| Local K8s | kind | `extraMounts` + `extraPortMappings`; scriptable; already used in the existing local sandbox | minikube, k3d, OrbStack K8s |
| Auth | SimpleAuthManager (Airflow 3 default) | Config-only users, no DB; built for dev/test | FAB auth manager |
| DAG source | LocalDagBundle on a hostPath PVC | Zero extra services | GitDagBundle, S3DagBundle (later) |
| Metrics | Airflow StatsD → statsd-exporter → Prometheus → Grafana (kube-prometheus-stack in full) | Same pipeline as the EKS setup's StatsD path; one mapping file for both modes | Airflow OTel metrics (chart has optional OTel service since 1.22; revisit in v2) |
| Page build | Vite + TypeScript | Standard; ruff WASM docs state Vite compatibility | webpack, no-build |
| UI framework | Svelte 5 | Small bundle, little boilerplate; Svelte Flow exists for the v2 visual builder | React (heavier), Lit |
| Editor | CodeMirror 6 | Modular, small, first-class lint gutter API | Monaco (large, worker setup) |
| Lint | `@astral-sh/ruff-wasm-web` | Real ruff in browser; `Workspace.check()` returns diagnostics | Pyodide + flake8 (slow, large) |
| Unit tests | Vitest | Native to Vite | Jest |
| Script checks | shellcheck, `helm lint`, `helm template` + kubeconform | No cluster needed | — |

> **Warning.** The ruff WASM API is labelled experimental by Astral. Pin the exact npm version and wrap it behind `web/src/lint/ruff.ts` so an API change touches one file.

---

## Repository Layout

```
dagbox/
├── README.md                     # This spec
├── DEVELOPER.md                  # Dev loop, how to run checks, how to debug the cluster
├── BURNLOG.md                    # Mistakes and fixes, newest first
├── Makefile                      # up, down, status, page, check, gate0
├── .env.example                  # All DAGBOX_* vars, no secret values
├── .gitignore                    # .env.local, lite/auth/, lite/.env.generated, cluster/.values.generated.yaml, web/dist, node_modules
├── requirements.txt              # Extra Python packages baked into the Airflow image
├── image/
│   └── Dockerfile                # FROM apache/airflow:${AIRFLOW_VERSION}; used by both modes
├── lite/
│   ├── compose.yaml              # postgres, airflow, statsd-exporter, prometheus, grafana
│   ├── prometheus.yml            # static scrape of statsd-exporter
│   └── grafana/provisioning/     # datasource + dashboard providers
├── monitoring/
│   ├── statsd-mapping.yaml       # shared by lite exporter and full chart (AIDEV-VERIFY Q10)
│   └── dashboards/
│       ├── dagbox-overview.json  # Airflow metrics, both modes; written by us
│       └── dagbox-k8s.json       # pods, nodes; full mode only
├── scripts/
│   ├── bootstrap.sh              # up: idempotent install + self-test
│   ├── status.sh                 # health report + login self-test
│   ├── teardown.sh               # delete cluster, keep host folders
│   ├── apply.sh                  # plan / apply / rollback of dagbox.overrides.json
│   ├── config-schema.sh          # regenerate config/airflow-keys.json from the pinned image
│   └── lib.sh                    # logging, die(), require_cmd(), load_env()
├── config/
│   ├── keys.json                 # setting classes and locked keys (hand-maintained)
│   └── airflow-keys.json         # every valid [section] key + default, generated per pinned version
├── cluster/
│   ├── kind.yaml.tmpl            # rendered with envsubst
│   ├── airflow-values.yaml
│   ├── airflow-values-celery.yaml   # overlay for --celery
│   ├── monitoring-values.yaml
│   └── manifests/
│       ├── storage.yaml.tmpl     # hostPath PVs + PVCs (dags, logs)
│       ├── servicemonitor-statsd.yaml
│       └── grafana-dashboard-cm.yaml   # generated from monitoring/dashboards/
├── gate0/
│   └── gate0_mount_check.py      # DAG used by the Gate-0 commands
├── web/
│   ├── index.html
│   ├── package.json
│   ├── vite.config.ts
│   ├── src/
│   │   ├── main.ts
│   │   ├── App.svelte
│   │   ├── api/client.ts         # fetch wrapper, JWT in memory, re-login on 401
│   │   ├── api/types.ts          # response types for the endpoints used
│   │   ├── api/errors.ts         # named error classes
│   │   ├── fs/dagFolder.ts       # directory handle, permission, read/write/list
│   │   ├── lint/ruff.ts          # ruff WASM adapter → CodeMirror diagnostics
│   │   ├── editor/Editor.svelte
│   │   ├── views/DagList.svelte
│   │   ├── views/RunPanel.svelte # trigger, task grid, log viewer
│   │   ├── views/ImportErrors.svelte
│   │   ├── views/Connections.svelte  # list, create, edit, delete; password write-only
│   │   ├── views/Variables.svelte    # list, create, edit, delete
│   │   ├── views/Config.svelte       # settings editor, plan/apply commands, history, effective config
│   │   ├── config/overrides.ts       # read/write/validate dagbox.overrides.json and last-apply.json
│   │   ├── views/Admin.svelte
│   │   ├── views/Monitor.svelte
│   │   ├── settings.ts           # non-secret settings in IndexedDB
│   │   └── templates/            # starter DAGs (.py as raw strings)
│   └── tests/                    # Vitest suites
└── LICENSE                       # see Open Questions
```

---

## Prerequisites

| Requirement | Version / value | Notes |
|-------------|-----------------|-------|
| macOS | 14+ | Linux works but is not tested in v1 |
| Docker Desktop or OrbStack | current, Compose v2 | Lite: ≥ 2 CPU, 4 GB. Full: ≥ 4 CPU, 8 GB; 12 GB with Celery |
| Homebrew | current | bootstrap installs missing CLIs with `brew install` |
| kind | ≥ 0.27 | full mode only, via brew |
| kubectl | matching kind's node version | full mode only, via brew |
| Helm | ≥ 3.19 | full mode only. Airflow chart 1.22.0 raised its minimum Helm version to 3.19.0 |
| jq, envsubst (`gettext`), openssl | any | via brew |
| Node.js | 22 LTS | only to build the page |
| Browser | Chrome, Edge, or another Chromium | File System Access API is Chromium-only |

`$DAGBOX_DAGS_DIR` and `$DAGBOX_LOGS_DIR` must be under `/Users` so Docker Desktop shares them by default.

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/ejoliet/dagbox.git && cd dagbox

# 2. Optional: extra Python packages for your DAGs
echo "pandas" >> requirements.txt

# 3. Stand up a backend (writes .env.local, prints URLs + credentials)
make up                         # lite: Compose, LocalExecutor, Postgres, Grafana
# make up MODE=full             # kind, KubernetesExecutor
# make up MODE=full EXECUTOR=celery

# 4. Build and serve the page
make page               # http://localhost:5173

# 5. In the page: Admin → paste values printed by `make up` → Choose DAG folder
#    (pick the folder printed as DAGBOX_DAGS_DIR) → New DAG → Save → Trigger
```

---

## Configuration Reference

All values live in `.env.local` (git-ignored). `bootstrap.sh` creates it from `.env.example` on first run and fills generated secrets. The file is the single source of truth: scripts read it, and the admin panel is filled from the values bootstrap prints.

| Variable | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `DAGBOX_MODE` | enum | `lite` | | `lite` or `full`. Only one mode runs at a time (same host ports) |
| `DAGBOX_CLUSTER_NAME` | str | `dagbox` | | kind cluster name / Compose project name |
| `DAGBOX_AIRFLOW_VERSION` | str | `3.3.2` | | Airflow image tag. Set `3.1.8` for prod parity |
| `DAGBOX_CHART_VERSION` | str | pinned at Gate-0 | ✅ | Airflow Helm chart version (see Open Questions) |
| `DAGBOX_MONITORING_CHART_VERSION` | str | pinned at Gate-0 | ✅ | kube-prometheus-stack chart version |
| `DAGBOX_EXECUTOR` | enum | `KubernetesExecutor` | | Full mode only: `KubernetesExecutor` or `CeleryExecutor`. Lite is always `LocalExecutor` |
| `DAGBOX_PARALLELISM` | int | `8` | | Lite: `[core] parallelism`, max concurrent task processes |
| `DAGBOX_POSTGRES_IMAGE` | str | pinned at Gate-0 | ✅ | Lite Postgres image tag (see Q12) |
| `DAGBOX_POSTGRES_PASSWORD` | secret | generated | | Lite Postgres password |
| `DAGBOX_FERNET_KEY` | secret | generated | | Encrypts connection passwords and variables in both modes |
| `DAGBOX_STATSD_EXPORTER_IMAGE`, `DAGBOX_PROMETHEUS_IMAGE`, `DAGBOX_GRAFANA_IMAGE` | str | pinned at Gate-0 | ✅ | Lite monitoring images |
| `DAGBOX_HOME` | path | `~/dagbox` | | Holds `dagbox.overrides.json`, `last-apply.json`, `history/`. The page gets a folder handle to it |
| `DAGBOX_DAGS_DIR` | path | `~/dagbox/dags` | | Host DAG folder; must be under `/Users` |
| `DAGBOX_HISTORY_KEEP` | int | `10` | | Applied snapshots kept in `$DAGBOX_HOME/history/` |
| `DAGBOX_LOGS_DIR` | path | `~/dagbox/logs` | | Host task-log folder |
| `DAGBOX_PAGE_PORT` | int | `5173` | | Page port; baked into CORS origin |
| `DAGBOX_AIRFLOW_PORT` | int | `8080` | | Host port → api-server NodePort 30080 |
| `DAGBOX_GRAFANA_PORT` | int | `3000` | | Host port → Grafana NodePort 30300 |
| `DAGBOX_PROMETHEUS_PORT` | int | `9090` | | Host port → Prometheus NodePort 30090 |
| `DAGBOX_ADMIN_USER` | str | `admin` | | SimpleAuthManager admin user |
| `DAGBOX_ADMIN_PASSWORD` | secret | generated | | `openssl rand -base64 18` on first run |
| `DAGBOX_GRAFANA_ADMIN_PASSWORD` | secret | generated | | Grafana admin; anonymous users get Viewer |
| `DAGBOX_PARSE_INTERVAL_S` | int | `10` | | Used for dag-processor and bundle refresh intervals |
| `DAGBOX_IMAGE` | str | `dagbox-airflow:<sha8 of requirements.txt + version>` | | Local image tag loaded into kind |

> **Warning.** In full mode, changing `DAGBOX_PAGE_PORT`, `DAGBOX_DAGS_DIR`, or `DAGBOX_LOGS_DIR` requires `make down && make up`: mounts and port mappings are fixed when the kind cluster is created. In lite mode, `make up` recreates the affected containers.

`DAGBOX_FERNET_KEY` is generated once (`python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`) and must not change, or stored connections become unreadable. Bootstrap never regenerates it if present.

### Airflow settings applied (via chart `config:`)

```yaml
# cluster/airflow-values.yaml (excerpt). AIDEV-VERIFY: confirm key names against pinned chart.
executor: KubernetesExecutor          # overlay switches to CeleryExecutor
images:
  airflow:
    repository: dagbox-airflow
    tag: "${DAGBOX_IMAGE_TAG}"
    pullPolicy: IfNotPresent           # image is side-loaded with `kind load`
dags:
  persistence:
    enabled: true
    existingClaim: dagbox-dags
  gitSync:
    enabled: false
logs:
  persistence:
    enabled: true
    existingClaim: dagbox-logs
statsd:
  enabled: true
  overrideMappings: []                  # AIDEV-VERIFY Q10: load monitoring/statsd-mapping.yaml here so names match lite
apiServer:                              # AIDEV-VERIFY: key name in pinned chart (apiServer vs webserver)
  service:
    type: NodePort
    ports:
      - name: api-server
        port: 8080
        nodePort: 30080
fernetKey: "${DAGBOX_FERNET_KEY}"       # AIDEV-VERIFY: or fernetKeySecretName in pinned chart
config:
  core:
    auth_manager: airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
    simple_auth_manager_users: "${DAGBOX_ADMIN_USER}:admin"
    simple_auth_manager_passwords_file: /opt/airflow/dagbox-auth/passwords.json
  api:
    access_control_allow_origins: "http://localhost:${DAGBOX_PAGE_PORT}"
    access_control_allow_methods: "GET POST PATCH DELETE OPTIONS"
    access_control_allow_headers: "Authorization Content-Type"
  dag_processor:
    min_file_process_interval: ${DAGBOX_PARSE_INTERVAL_S}
    refresh_interval: ${DAGBOX_PARSE_INTERVAL_S}
```

SimpleAuthManager reads users from `[core] simple_auth_manager_users` and passwords from the file set in `[core] simple_auth_manager_passwords_file`. Bootstrap writes `{"<user>": "<password>"}` into Secret `dagbox-auth` and mounts it at that path on the api-server. See Open Question Q2 on read-only mounts.

### Lite mode compose (excerpt)

```yaml
# lite/compose.yaml. AIDEV-VERIFY Q9: standalone honors these settings with Postgres.
name: ${DAGBOX_CLUSTER_NAME}
x-airflow-env: &airflow-env
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${DAGBOX_POSTGRES_PASSWORD}@postgres:5432/airflow
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  AIRFLOW__CORE__PARALLELISM: ${DAGBOX_PARALLELISM}
  AIRFLOW__CORE__FERNET_KEY: ${DAGBOX_FERNET_KEY}
  AIRFLOW__CORE__LOAD_EXAMPLES: "False"
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "${DAGBOX_ADMIN_USER}:admin"
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE: /opt/airflow/dagbox-auth/passwords.json
  AIRFLOW__API__ACCESS_CONTROL_ALLOW_ORIGINS: http://localhost:${DAGBOX_PAGE_PORT}
  AIRFLOW__API__ACCESS_CONTROL_ALLOW_METHODS: "GET POST PATCH DELETE OPTIONS"
  AIRFLOW__API__ACCESS_CONTROL_ALLOW_HEADERS: "Authorization Content-Type"
  AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL: ${DAGBOX_PARSE_INTERVAL_S}
  AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: ${DAGBOX_PARSE_INTERVAL_S}
  AIRFLOW__METRICS__STATSD_ON: "True"
  AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
  AIRFLOW__METRICS__STATSD_PORT: "9125"
  AIRFLOW__METRICS__STATSD_PREFIX: airflow
services:
  postgres:
    image: ${DAGBOX_POSTGRES_IMAGE}
    environment: { POSTGRES_USER: airflow, POSTGRES_PASSWORD: "${DAGBOX_POSTGRES_PASSWORD}", POSTGRES_DB: airflow }
    volumes: [dagbox-pg:/var/lib/postgresql/data]
    healthcheck: { test: ["CMD", "pg_isready", "-U", "airflow"], interval: 5s, retries: 20 }
  airflow:
    image: ${DAGBOX_IMAGE}
    command: standalone
    environment: *airflow-env
    env_file:                                  # rendered by `make apply`; absent until first apply
      - path: .env.generated
        required: false
    depends_on: { postgres: { condition: service_healthy } }
    ports: ["127.0.0.1:${DAGBOX_AIRFLOW_PORT}:8080"]
    volumes:
      - ${DAGBOX_DAGS_DIR}:/opt/airflow/dags
      - ${DAGBOX_LOGS_DIR}:/opt/airflow/logs
      - ./auth:/opt/airflow/dagbox-auth        # passwords.json written by bootstrap; git-ignored
  statsd-exporter:
    image: ${DAGBOX_STATSD_EXPORTER_IMAGE}
    command: ["--statsd.mapping-config=/etc/statsd/mapping.yaml"]
    volumes: [../monitoring/statsd-mapping.yaml:/etc/statsd/mapping.yaml:ro]
  prometheus:
    image: ${DAGBOX_PROMETHEUS_IMAGE}
    volumes: [./prometheus.yml:/etc/prometheus/prometheus.yml:ro]
    ports: ["127.0.0.1:${DAGBOX_PROMETHEUS_PORT}:9090"]
  grafana:
    image: ${DAGBOX_GRAFANA_IMAGE}
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${DAGBOX_GRAFANA_ADMIN_PASSWORD}
      GF_SECURITY_ALLOW_EMBEDDING: "true"
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ../monitoring/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["127.0.0.1:${DAGBOX_GRAFANA_PORT}:3000"]
volumes:
  dagbox-pg: {}
```

`lite/auth/` is created by bootstrap with `chmod 700`, holds only `passwords.json`, and is git-ignored. Full mode keeps the Kubernetes Secret approach.

### kind config

```yaml
# cluster/kind.yaml.tmpl
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: ${DAGBOX_DAGS_DIR}
        containerPath: /dagbox/dags
      - hostPath: ${DAGBOX_LOGS_DIR}
        containerPath: /dagbox/logs
    extraPortMappings:                 # listenAddress 127.0.0.1: never expose to the LAN
      - { containerPort: 30080, hostPort: ${DAGBOX_AIRFLOW_PORT},    listenAddress: "127.0.0.1" }
      - { containerPort: 30300, hostPort: ${DAGBOX_GRAFANA_PORT},    listenAddress: "127.0.0.1" }
      - { containerPort: 30090, hostPort: ${DAGBOX_PROMETHEUS_PORT}, listenAddress: "127.0.0.1" }
```

### Monitoring values

```yaml
# cluster/monitoring-values.yaml (excerpt)
prometheus:
  service: { type: NodePort, nodePort: 30090 }
  prometheusSpec:
    serviceMonitorSelectorNilUsesHelmValues: false   # pick up our ServiceMonitor without a release label
grafana:
  adminPassword: "${DAGBOX_GRAFANA_ADMIN_PASSWORD}"
  service: { type: NodePort, nodePort: 30300 }
  grafana.ini:
    security: { allow_embedding: true }
    auth.anonymous: { enabled: true, org_role: Viewer }
  sidecar:
    dashboards: { enabled: true, label: grafana_dashboard }
alertmanager:
  enabled: false
```

---

## API / Interface Contract

### Make targets (user-facing CLI)

```
make up [MODE=lite|full] [EXECUTOR=celery]
                            Run scripts/bootstrap.sh. Idempotent. Re-run to apply changes.
                            Refuses to start one mode while the other is running.
make down [PURGE=1]         Run scripts/teardown.sh. Lite: compose down (PURGE=1 also drops the
                            Postgres volume). Full: delete cluster. Host folders and .env.local kept.
make status                 Run scripts/status.sh.
make page                   Build web/ and serve dist on 127.0.0.1:$DAGBOX_PAGE_PORT (vite preview).
make check                  Agent-safe checks: vitest, shellcheck, helm lint/template, kubeconform. No cluster.
make plan                   Validate $DAGBOX_HOME/dagbox.overrides.json and print the diff + restart class. Changes nothing.
make apply                  plan, snapshot, apply, self-test, write $DAGBOX_HOME/last-apply.json.
make rollback [TO=<ts>]     Re-apply the previous snapshot (or the named one) through the same path.
make config-schema          Regenerate config/airflow-keys.json from the pinned image (run after a version change).
make gate0 [MODE=...]       Print the Gate-0 commands for that mode (does not run them).
```

### `bootstrap.sh` steps (in order, each idempotent)

1. Load or create `.env.local`; generate missing secrets; `chmod 600`.
2. `require_cmd` for docker, kind, kubectl, helm, jq, envsubst, openssl. Offer `brew install` for missing ones. Check Docker is running and has ≥ 8 GB.
3. Create `$DAGBOX_HOME` (with `history/`), `$DAGBOX_DAGS_DIR`, and `$DAGBOX_LOGS_DIR`. Seed `$DAGBOX_HOME/dagbox.overrides.json` with `{"version": 1}` if absent. In full mode, include `cluster/.values.generated.yaml` in the Helm install if it exists, so `make up` never undoes an applied change. Copy `gate0/gate0_mount_check.py` into the DAG folder if absent.
4. Build `image/Dockerfile` as `$DAGBOX_IMAGE` if that tag does not exist locally.
   - **Lite:** write `lite/auth/passwords.json`, run `docker compose -f lite/compose.yaml up -d --wait`, skip to step 9.
   - **Full:** continue with steps 5–8.
5. Create the kind cluster from the rendered template if it does not exist. If it exists but mounts or ports differ from `.env.local`, stop and tell the user to run `make down`.
6. `kind load docker-image $DAGBOX_IMAGE --name $DAGBOX_CLUSTER_NAME`.
7. Apply `storage.yaml` (PVs/PVCs), Secret `dagbox-auth`, ServiceMonitor, dashboard ConfigMap.
8. `helm upgrade --install` kube-prometheus-stack in `monitoring`, then Airflow in `airflow` (`--wait --timeout 15m`), with the celery overlay if requested.
9. Self-test: `POST /auth/token`, `GET /api/v2/dags`, CORS preflight. Fail with a named message on each.
10. Print a block with page URL, Airflow URL, Grafana URL, admin user, admin password, DAG folder path.

Exit codes: `0` ok, `10` missing tool, `11` Docker not running or too small, `12` cluster config drift, `20` Helm install failed, `30` self-test failed.

### Airflow REST API used by the page

Base: `http://localhost:$DAGBOX_AIRFLOW_PORT`. All calls except login send `Authorization: Bearer <jwt>`.

| Purpose | Request | Notes |
|---------|---------|-------|
| Login | `POST /auth/token` body `{"username","password"}` | Returns `{"access_token": "..."}` |
| List DAGs | `GET /api/v2/dags?limit=100` | Poll after save until the new `dag_id` appears |
| Unpause | `PATCH /api/v2/dags/{dag_id}` body `{"is_paused": false}` | Called before every trigger |
| Trigger | `POST /api/v2/dags/{dag_id}/dagRuns` body `{"logical_date": null, "conf": {}}` | Omitting `logical_date` returns 422 |
| Run state | `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}` | Poll every 2 s while not terminal |
| Task states | `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances` | Same poll |
| Task log | `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}` | AIDEV-VERIFY: response shape (JSON `content` vs text); handle both |
| Import errors | `GET /api/v2/importErrors` | Show per file; match to saved file name |
| Effective config | `GET /api/v2/config` | Read-only; only if `expose_config` allows (Q13) |
| List connections | `GET /api/v2/connections?limit=100` | Never display a returned password field even if present (Q11) |
| Create connection | `POST /api/v2/connections` body `{connection_id, conn_type, host, port, login, password, schema, extra}` | `extra` is a JSON string |
| Update connection | `PATCH /api/v2/connections/{connection_id}` | Send `password` only if the user typed a new one; AIDEV-VERIFY `update_mask` behavior |
| Delete connection | `DELETE /api/v2/connections/{connection_id}` | Confirm dialog |
| List variables | `GET /api/v2/variables?limit=100` | Values of keys matching Airflow's sensitive names come back masked; show as masked |
| Create variable | `POST /api/v2/variables` body `{key, value, description}` | |
| Update variable | `PATCH /api/v2/variables/{key}` | |
| Delete variable | `DELETE /api/v2/variables/{key}` | Confirm dialog |

Cross-check every path against `GET /api/v2/openapi.json` on the pinned version during Gate-0.

### Page behavior

| Area | Behavior |
|------|----------|
| Auth | JWT held in a module variable only. Never in localStorage, sessionStorage, IndexedDB, or URL. On 401: one silent re-login with the password the user entered this session, then retry once; if it fails, show the login form. The password is also memory-only. |
| Settings | Non-secret settings (URLs, user name, executor label) in IndexedDB. Directory handle in IndexedDB. |
| DAG folder | `showDirectoryPicker({ mode: "readwrite" })`. On load, `queryPermission`; if not granted, show a "Reconnect folder" button that calls `requestPermission` (needs a user gesture). |
| Save | Write `<name>.py` atomically: write to `.<name>.py.tmp`, then move. If `move()` is unsupported, write directly. Lint must have no `F`/`E9` errors; `AIR` findings warn but do not block. |
| Upload | Drop a `.py` onto the page → open in editor → same save path. |
| After save | Poll `GET /dags` and `GET /importErrors` every 3 s for up to 60 s. Show "Waiting for parse", then the DAG or its import error. |
| Run | Unpause, trigger, live task grid (state colors), click a task for its log. Stop polling on terminal state. |
| Connections | Table with search; form for create/edit. `conn_type` free text with suggestions (`aws`, `postgres`, `http`, `fs`). Password field is write-only: empty means unchanged. `extra` edited as JSON with validation before submit. Nothing from this view is stored in the browser. |
| Variables | Table with search; inline edit; JSON values validated if they parse as JSON, saved as string otherwise. |
| Monitor | Grafana `dagbox-overview` in an iframe at `/d/dagbox-overview?kiosk`; in full mode a second tab for `dagbox-k8s`. If the iframe fails to load, show a link instead. |
| Config | Needs a folder handle to `$DAGBOX_HOME` (separate from the DAG folder). Form grouped by `dagbox`, Airflow sections, `helm` (full only). Add-key autocomplete from `airflow-keys.json` (bundled at build) with defaults shown. Each key shows its class badge; locked keys are read-only. "Save" validates with the same rules as `apply.sh`, writes the file, then shows `make plan` and `make apply` with a copy button. The view then watches `last-apply.json` (every 2 s, 15 min max) and API health, and shows ok / failed / rolled back. History lists `history/` snapshots with a generated `make rollback TO=<ts>` command. |
| Mode badge | Admin panel shows `lite` or `full` (from settings) so users know what parallelism means for their run. |
| Templates | `hello_taskflow.py`, `fan_out_mapping.py` (dynamic task mapping with `.expand`), `fail_and_retry.py` (one task fails once, retries, succeeds). |

Lint config passed to `new Workspace(...)`:

```ts
// AIDEV-NOTE: rule set is the product's opinion; keep in sync with DEVELOPER.md.
{ "line-length": 100, lint: { select: ["F", "E9", "AIR"] } }
```

### Grafana dashboards

Written by us as JSON in `monitoring/dashboards/`. `dagbox-overview` works in both modes because both use `monitoring/statsd-mapping.yaml`. `dagbox-k8s` holds the pod and node panels and is provisioned only in full mode. Panels and the metrics they need:

| Panel | Metric (StatsD name, Prometheus form after exporter) |
|-------|------------------------------------------------------|
| Scheduler heartbeat | `scheduler_heartbeat` |
| DAG parse time | `dag_processing.total_parse_time` |
| Import errors | `dag_processing.import_errors` |
| Task outcomes / min | `ti_successes`, `ti_failures` |
| Executor slots | `executor.open_slots`, `executor.running_tasks`, `executor.queued_tasks` |
| Pods in `airflow` ns (`dagbox-k8s`, full only) | kube-state-metrics `kube_pod_status_phase` |
| Node CPU / memory (`dagbox-k8s`, full only) | node-exporter |

> **Warning.** Exact exported names depend on the chart's statsd-exporter mapping and the Airflow version. Gate-0 step 7 lists the real names; write the dashboard JSON only after that.

---

### Config editor and lifecycle (v1)

The page edits settings; the user runs one command; the page shows the result. No process other than the existing backend runs.

```
 Page Config view ──writes──► $DAGBOX_HOME/dagbox.overrides.json
        │                              │
        │ shows "make apply"           ▼
        │                     make apply (user runs)
        │                       validate → plan → snapshot → apply → self-test
        │                              │
        ◄──polls───────────── $DAGBOX_HOME/last-apply.json  +  GET /api/v2/dags (health)
```

**Overrides file** (`dagbox.overrides.json`, JSON so scripts need only `jq`):

```json
{
  "version": 1,
  "dagbox": { "DAGBOX_PARALLELISM": 16, "DAGBOX_PARSE_INTERVAL_S": 5 },
  "airflow": {
    "core": { "max_active_tasks_per_dag": 16 },
    "scheduler": { "parsing_cleanup_interval": 60 }
  },
  "helm": { "workers": { "resources": { "limits": { "memory": "2Gi" } } } }
}
```

| Block | Lite | Full |
|-------|------|------|
| `dagbox` | Allowlisted non-secret `DAGBOX_*` keys, merged over `.env.local` | Same |
| `airflow` | Rendered as `AIRFLOW__SECTION__KEY` env into `lite/.env.generated` | Rendered into the chart's `config:` block in `cluster/.values.generated.yaml` |
| `helm` | Ignored, with a warning | Merged last into the values overlay; top-level keys limited to an allowlist in `keys.json` (`workers`, `scheduler`, `dagProcessor`, `apiServer`, `triggerer`, `statsd`, `resources`) |

**Setting classes** (`config/keys.json`, shared by page and scripts):

| Class | Meaning | Lite action | Full action |
|-------|---------|-------------|-------------|
| `restart` | Picked up on process restart (default for Airflow keys) | `docker compose up -d` recreates `airflow` only | `helm upgrade` rolls affected deployments |
| `recreate` | Mount, port, or CORS origin changes | `make down && make up` printed instead of applying | Same |
| `migration` | `DAGBOX_AIRFLOW_VERSION` | Snapshot DB (`pg_dump` to `history/`), then apply; DB migrates on start | `helm upgrade` runs the chart's migration job |
| `locked` | Fernet key, DB connection, auth manager and users, passwords file, executor (use `make up EXECUTOR=`) | Rejected | Rejected |

Unknown `[section] key` pairs (not in `config/airflow-keys.json`) are rejected, which catches typos before a restart. Any key whose name matches `password|secret|token|fernet|key$|conn` is rejected with "use Connections, Variables, or `.env.local`".

**`apply.sh` steps**

1. Validate with `jq` against `keys.json` and `airflow-keys.json`. Exit `40` on failure with the offending keys.
2. Render the target (`lite/.env.generated` or `cluster/.values.generated.yaml`) to a temp file. Print `diff -u` against the last applied render and the highest setting class involved. In full mode also diff `helm template` output.
3. If the class is `recreate`, print the `make down && make up` command and exit `41` without changing anything.
4. Snapshot the current overrides and renders to `history/<UTC timestamp>/`. Prune to `DAGBOX_HISTORY_KEEP`.
5. Apply. Lite: `docker compose up -d --wait`. Full: `helm upgrade --install --wait --timeout 10m` with the overlay, plus automatic rollback on failure (AIDEV-VERIFY Q14: `--atomic` vs `--rollback-on-failure` in the pinned Helm).
6. Self-test (login, `GET /dags`, CORS preflight). On failure: lite re-applies the previous snapshot; full relies on step 5's rollback. Exit `42`.
7. Write `last-apply.json`: `{ "status": "ok|failed|rolled_back", "started_at", "finished_at", "class", "snapshot", "message" }`.

**Effective config tab.** The page reads `GET /api/v2/config` when exposed. Both modes set `[api] expose_config` to the non-sensitive option (AIDEV-VERIFY Q13). If the endpoint returns 403 or 404, the tab shows "not exposed" and the editor still works.

## Error Handling

### Page (`web/src/api/errors.ts`)

| Error | When | User-facing message | Retry |
|-------|------|---------------------|-------|
| `NetworkError` | `fetch` throws `TypeError` | "Can't reach Airflow at <url>. Is the cluster up? Run `make status`. If it is up, the CORS origin may not match this page's port." | Manual |
| `AuthError` | 401 after one re-login | "Login failed. Check user and password from `make up` output." | No |
| `ValidationError` | 422 | Show `detail` from the response body | No |
| `OverridesInvalidError` | Page-side validation fails | Lists offending keys and the rule each broke | No |
| `ConflictError` | 409 on create connection or variable | "<id> already exists. Edit it instead?" | No |
| `NotFoundError` | 404 on a DAG | "DAG not parsed yet" while within the 60 s parse window, then "Not found" | Poll |
| `ApiError` | other 4xx/5xx | Status + body excerpt | Manual |
| `FolderPermissionError` | handle permission not granted | "Reconnect folder" button | User gesture |
| `UnsupportedBrowserError` | no `showDirectoryPicker` | "Use Chrome or Edge to save DAGs. Run and monitor still work." | No |
| `LintEngineError` | ruff WASM fails to init | Editor works without lint; banner shown | No |

### Scripts

`apply.sh` exit codes: `40` invalid overrides, `41` needs recreate (nothing changed), `42` apply or self-test failed (rolled back), `43` rollback target not found.

`lib.sh` provides `die <code> <message>`. Every failure prints what failed, the likely cause, and the next command to run. No silent failures; `set -euo pipefail` everywhere.

---

## Security

- Every host port binds to `127.0.0.1` via `listenAddress`. Nothing is reachable from the LAN.
- `.env.local` is git-ignored and `chmod 600`. No secret appears in committed files, Helm values files, or the page bundle.
- Secrets reach the cluster only as Kubernetes Secrets created by bootstrap.
- JWT and password exist only in page memory for the session.
- Connection passwords and variable values are sent to Airflow and never stored in the browser. Airflow encrypts them with `DAGBOX_FERNET_KEY`.
- Lite Postgres is not published to the host. Only Airflow, Prometheus, and Grafana ports are, all on `127.0.0.1`.
- The config editor cannot execute anything. It writes a JSON file; the user runs the command. Secret-like keys and locked keys are rejected by both the page and `apply.sh`.
- Grafana anonymous access is Viewer only and only on `127.0.0.1`.
- SimpleAuthManager is a development auth manager by design. dagbox is not for shared or remote use (see Non-Goals).

---

## Testing

| Command | Who runs it | Covers |
|---------|-------------|--------|
| `make check` | Agent and Emmanuel | Vitest, shellcheck, `apply.sh` validation and render against fixture overrides in `tests/fixtures/` (no Docker, no cluster), `docker compose -f lite/compose.yaml config -q` with `.env.example`, `helm lint` + `helm template` piped to kubeconform, JSON lint of dashboards |
| `make gate0` | Prints commands for Emmanuel | Cluster-level checks listed below |
| Browser checks | Emmanuel | Acceptance criteria 2–8, 10–12 |

Vitest suites (`web/tests/`):

| Suite | Covers |
|-------|--------|
| `client.test.ts` | Bearer header; 401 → one re-login → retry; 422 → `ValidationError`; `TypeError` → `NetworkError`; token never written to any storage (spy on `localStorage`, `sessionStorage`, `indexedDB`) |
| `trigger.test.ts` | Trigger body always contains `"logical_date": null` |
| `dagFolder.test.ts` | Permission states; tmp-then-move write; fallback write (mocked handles) |
| `ruff.test.ts` | Adapter maps ruff diagnostics to CodeMirror ranges; `AIR` rules present in the pinned build |
| `parseWait.test.ts` | Poll stops on appear, on import error, and at 60 s |
| `overrides.test.ts` | Locked and secret-like keys rejected; unknown Airflow keys rejected; class computed as the highest of all changed keys; `helm` block ignored in lite with a warning |
| `connections.test.ts` | PATCH omits `password` when unchanged; invalid `extra` JSON blocked before request; 409 → `ConflictError`; nothing written to any storage |
| `variables.test.ts` | Create/update/delete request shapes; masked values never overwritten by an unedited save |

### Gate-0 commands (for Emmanuel, run in order)

Run once with `make up` (lite), then again after `make down && make up MODE=full`. Steps marked *(full)* apply only to full mode.

```bash
set -a; source .env.local; set +a
A=http://localhost:$DAGBOX_AIRFLOW_PORT

# 1. Login returns a token
TOKEN=$(curl -sf -X POST $A/auth/token -H 'Content-Type: application/json' \
  -d "{\"username\":\"$DAGBOX_ADMIN_USER\",\"password\":\"$DAGBOX_ADMIN_PASSWORD\"}" | jq -r .access_token)
test -n "$TOKEN" && echo OK-login

# 2. API works and the Gate-0 DAG is parsed
curl -sf $A/api/v2/dags -H "Authorization: Bearer $TOKEN" | jq -r '.dags[].dag_id'

# 3. CORS preflight from the page origin (expect access-control-allow-origin: http://localhost:5173)
curl -si -X OPTIONS $A/api/v2/dags -H "Origin: http://localhost:$DAGBOX_PAGE_PORT" \
  -H 'Access-Control-Request-Method: GET' -H 'Access-Control-Request-Headers: authorization' \
  | grep -i '^access-control-allow-origin'

# 4. Unpause + trigger (expect 2xx and a dag_run_id)
curl -sf -X PATCH $A/api/v2/dags/gate0_mount_check -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"is_paused": false}' | jq .is_paused
RUN=$(curl -sf -X POST $A/api/v2/dags/gate0_mount_check/dagRuns -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}' | jq -r .dag_run_id)
echo "$RUN"

# 5. New file appears within 30 s
sed 's/gate0_mount_check/gate0_new_file/' gate0/gate0_mount_check.py > "$DAGBOX_DAGS_DIR/gate0_new_file.py"
for i in $(seq 1 15); do curl -sf $A/api/v2/dags -H "Authorization: Bearer $TOKEN" \
  | jq -e '.dags[] | select(.dag_id=="gate0_new_file")' >/dev/null && echo "OK-parse ${i}x2s" && break; sleep 2; done

# 6. Task saw the host file and its log is readable (full: after the task pod is gone)
sleep 60; kubectl -n airflow get pods 2>/dev/null   # (full) task pod should be gone
curl -sf "$A/api/v2/dags/gate0_mount_check/dagRuns/$RUN/taskInstances/list_dags/logs/1" \
  -H "Authorization: Bearer $TOKEN" | head -c 2000   # expect the DAG folder listing
ls "$DAGBOX_LOGS_DIR"                              # log files visible on the host

# 7. Airflow metrics reach Prometheus (also prints real metric names for the dashboard)
curl -s "http://localhost:$DAGBOX_PROMETHEUS_PORT/api/v1/label/__name__/values" \
  | jq -r '.data[] | select(startswith("airflow"))'

# 8. Connections and variables round-trip (expect 2xx; password not echoed back)
H=(-H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json')
curl -sf -X POST $A/api/v2/connections "${H[@]}" \
  -d '{"connection_id":"gate0_conn","conn_type":"http","host":"example.org","password":"s3cret"}' | jq
curl -sf $A/api/v2/connections/gate0_conn "${H[@]}" | jq '{connection_id, password}'
curl -sf -X DELETE $A/api/v2/connections/gate0_conn "${H[@]}" -o /dev/null -w '%{http_code}\n'
curl -sf -X POST $A/api/v2/variables "${H[@]}" -d '{"key":"gate0_var","value":"42"}' | jq
curl -sf -X DELETE $A/api/v2/variables/gate0_var "${H[@]}" -o /dev/null -w '%{http_code}\n'

# 9. (lite) Parallel tasks actually overlap: trigger fan_out_mapping, then check overlapping start/end times
#    in the task grid, or: docker compose -p $DAGBOX_CLUSTER_NAME top airflow

# 10. Config lifecycle: change a restart-class key, apply, verify, roll back
echo '{"version":1,"airflow":{"core":{"max_active_tasks_per_dag":4}}}' > "$DAGBOX_HOME/dagbox.overrides.json"
make plan && make apply && jq . "$DAGBOX_HOME/last-apply.json"
docker compose -p $DAGBOX_CLUSTER_NAME exec airflow airflow config get-value core max_active_tasks_per_dag    # (lite) expect 4
kubectl -n airflow exec deploy/airflow-scheduler -- airflow config get-value core max_active_tasks_per_dag   # (full) expect 4
make rollback && jq .status "$DAGBOX_HOME/last-apply.json"
curl -s $A/api/v2/config -H "Authorization: Bearer $TOKEN" -o /dev/null -w '%{http_code}\n'   # Q13

# 11. ruff WASM exposes AIR rules (run in web/)
cd web && npx vitest run tests/ruff.test.ts
```

`gate0/gate0_mount_check.py`: one TaskFlow task `list_dags` that prints `os.listdir("/opt/airflow/dags")` (AIDEV-VERIFY: the chart's DAG mount path) and the first line of its own file.

---

## Non-Goals (v1)

- Running Airflow or tasks in the browser (Pyodide cannot host Airflow).
- Git, S3, or GCS DAG bundles and DAG versioning (v2 candidate: GitDagBundle with a local Gitea).
- Visual DAG builder (v2 candidate: Svelte Flow canvas → TaskFlow code, one-way).
- Hosting the page on GitHub Pages or any non-localhost origin. Chrome's Local Network Access permission gating makes public-site → localhost calls unreliable.
- Multi-node kind, remote clusters, EKS targets.
- Multiple users, roles beyond one admin, SSO.
- Linux and Windows support (Linux likely works; untested).
- Testing connections from the page (`POST /connections/test` is disabled by default in Airflow; not enabled by dagbox).
- Per-task isolation in lite mode. Use full mode for that.
- Running lite and full at the same time.
- One-click apply from the page. v1 requires the user to run `make apply`; see "v2: Control agent".
- Editing secrets through the config editor.
- Alerting (Alertmanager disabled).

---

## Open Questions

Resolve during Gate-0. The agent must not guess these; it leaves an `AIDEV-VERIFY` marker at each use. Q9, Q11–Q13, Q15 block lite mode; Q1–Q7, Q10, Q14 block full mode.

- [ ] **Q1 — Chart pin.** Which Airflow Helm chart version to pin with Airflow 3.3.2. Chart 1.22.0 defaults to 3.2.2; a later chart defaults to 3.3.0. Run `helm repo update && helm search repo apache-airflow/airflow --versions | head` and pin the newest; override the image tag either way.
- [ ] **Q2 — Passwords file mount.** Does SimpleAuthManager accept a read-only Secret mount for `simple_auth_manager_passwords_file`, or does it need to write it? Fallback: an init container copies the Secret into an `emptyDir`.
- [ ] **Q3 — Chart value keys.** `apiServer` vs `webserver` keys, the DAG mount path in pods, and whether `dags.persistence.existingClaim` also mounts into KubernetesExecutor task pods in the pinned chart (Gate-0 step 6 proves it).
- [ ] **Q4 — Bundled Postgres.** Is the chart's bundled PostgreSQL image still pullable in the pinned chart? If not, deploy a minimal Postgres StatefulSet in `cluster/manifests/` and point `data.metadataConnection` at it.
- [ ] **Q5 — Host-folder permissions.** Can uid 50000 in task pods write to `$DAGBOX_LOGS_DIR` through Docker Desktop / OrbStack file sharing? If not, an init container sets ownership, or logs move to a node-local path (and lose host visibility).
- [ ] **Q6 — CORS header format.** Separator for `access_control_allow_methods` / `_headers` in the pinned version.
- [ ] **Q7 — Log endpoint shape** in the pinned version (JSON with `content` vs plain text).
- [ ] **Q8 — License.** MIT for the repo? (Not assumed.)
- [ ] **Q9 — `airflow standalone` with Postgres and LocalExecutor.** Does standalone honor the configured DB, executor, and auth settings in the pinned version, and run DB migrations on start? Fallback: split Compose services (`db migrate` init job, then `api-server`, `scheduler`, `dag-processor`, `triggerer`) from the same image.
- [ ] **Q10 — Shared StatsD mapping.** How the pinned chart accepts a custom mapping (`statsd.overrideMappings` or similar) so full-mode metric names match lite. If it can't, the dashboard uses a mode variable with two metric names per panel.
- [ ] **Q11 — Connection password in API responses.** Is `password` returned, masked, or omitted by `GET /connections/{id}` in the pinned version? The page never displays it either way.
- [ ] **Q12 — Postgres version.** Highest Postgres major version supported by the pinned Airflow; pin `DAGBOX_POSTGRES_IMAGE` to it.
- [ ] **Q13 — Exposing config.** Exact key and allowed values for `[api] expose_config` in the pinned version (a non-sensitive-only value is expected), and the response shape of `GET /api/v2/config`.
- [ ] **Q14 — Helm auto-rollback flag.** `--atomic` or its renamed equivalent for the Helm version required by the pinned chart.
- [ ] **Q15 — Key list generation.** Whether `airflow config list --defaults` in the pinned image prints every valid key in a parseable form for `config-schema.sh`.

---

## Agent Build Instructions

> Implement end-to-end using only this README. Leave `AIDEV-VERIFY` markers where Open Questions apply.
> **Do not** start a cluster, run Docker builds, open a browser, push to GitHub, or install Homebrew packages. Emmanuel runs those. When a step needs them, write the exact command into `DEVELOPER.md` and stop that step.

### Build Order

| Phase | Deliverable | Done when |
|-------|-------------|-----------|
| 0 | Scaffold: Makefile, `.env.example`, `.gitignore`, `web/` Vite + Svelte + TS, Vitest, DEVELOPER.md, BURNLOG.md | `make check` passes on the empty project |
| 1a | `scripts/lib.sh`, `bootstrap.sh` (lite path), `status.sh`, `teardown.sh`; `lite/`; `monitoring/statsd-mapping.yaml`; `image/Dockerfile`; `gate0/` DAG | shellcheck clean; `docker compose config -q` passes; `make gate0` prints the block |
| — | **Stop. Emmanuel runs `make up` (lite) and Gate-0; resolves Q9, Q11, Q12; records results in BURNLOG.md** | Lite Gate-0 steps pass |
| 1b | Full path in `bootstrap.sh`; `cluster/` templates, values, manifests | `helm template` + kubeconform pass |
| — | **Stop. Emmanuel runs `make up MODE=full` and Gate-0; resolves Q1–Q7, Q10** | Full Gate-0 steps pass |
| 2 | `api/` client, errors, types; `fs/dagFolder.ts`; `lint/ruff.ts` | Vitest suites for these pass |
| 3 | Views: Admin, Editor (with templates and drop-to-upload), DagList, ImportErrors, RunPanel with log viewer, Connections, Variables | Vitest passes; `make page` builds |
| 3b | `config/keys.json`; `scripts/apply.sh` (lite path, then full path); `config-schema.sh`; Config view with history and effective-config tab | `overrides.test.ts` passes; `apply.sh` fixture renders match golden files; shellcheck clean |
| 4 | Dashboards from Gate-0 metric names; lite provisioning; full ConfigMap generation; Monitor view | JSON valid; ConfigMap renders; compose config valid |
| 5 | README Quick Start re-checked against real script output | Emmanuel confirms acceptance criteria |

### File Map

| File | Purpose | Key symbols |
|------|---------|-------------|
| `web/src/api/client.ts` | All HTTP | `login()`, `request<T>()`, `listDags()`, `unpause()`, `triggerRun()`, `getRun()`, `listTaskInstances()`, `getTaskLog()`, `listImportErrors()` |
| `web/src/api/errors.ts` | Error classes | `NetworkError`, `AuthError`, `ValidationError`, `NotFoundError`, `ApiError` |
| `web/src/fs/dagFolder.ts` | Folder access | `pickFolder()`, `restoreFolder()`, `ensurePermission()`, `writeDag()`, `listDagFiles()`, `FolderPermissionError`, `UnsupportedBrowserError` |
| `web/src/lint/ruff.ts` | Lint adapter | `initLint()`, `lintPython(src): Diagnostic[]`, `LintEngineError` |
| `web/src/api/client.ts` (cont.) | Connections, variables | `listConnections()`, `createConnection()`, `updateConnection()`, `deleteConnection()`, `listVariables()`, `createVariable()`, `updateVariable()`, `deleteVariable()` |
| `lite/compose.yaml` | Lite backend | services `postgres`, `airflow`, `statsd-exporter`, `prometheus`, `grafana` |
| `monitoring/statsd-mapping.yaml` | Shared metric names | mappings for scheduler, dag_processing, ti, executor, pool |
| `scripts/apply.sh` | Config lifecycle | subcommands `plan`, `apply`, `rollback`; functions `validate()`, `render_lite()`, `render_full()`, `snapshot()`, `self_test()`, `write_result()` |
| `config/keys.json` | Setting classes | `{ "locked": [...], "recreate": [...], "migration": [...], "dagbox_allow": [...], "helm_allow": [...], "secret_pattern": "..." }` |
| `web/src/config/overrides.ts` | Page side of lifecycle | `loadOverrides()`, `validateOverrides()`, `classify()`, `saveOverrides()`, `watchLastApply()`, `listHistory()` |
| `web/src/settings.ts` | IndexedDB settings | `loadSettings()`, `saveSettings()` — no secrets |
| `web/src/views/RunPanel.svelte` | Trigger + monitor | 2 s poll loop, terminal-state stop |
| `scripts/lib.sh` | Shared shell | `log()`, `die()`, `require_cmd()`, `load_env()`, `render()` |
| `scripts/bootstrap.sh` | `make up` | Steps 1–10 above |
| `cluster/manifests/storage.yaml.tmpl` | PVs/PVCs | `dagbox-dags`, `dagbox-logs` (hostPath `/dagbox/*`, RWO, `storageClassName: ""`) |
| `cluster/airflow-values-celery.yaml` | Overlay | `executor: CeleryExecutor`, `redis.enabled: true`, `workers.replicas: 1` |

### Constraints

- TypeScript `strict: true`. No `any` in `api/`.
- No secret in any committed file, bundle, or log line. Scripts never `echo` a password except the final summary block.
- Shell: bash, `set -euo pipefail`, shellcheck clean, macOS default tools plus the listed brew tools only.
- All versions pinned in one place: `.env.example` (charts, Airflow) and `web/package.json` (exact npm versions, no `^`).
- `AIDEV-NOTE` / `AIDEV-VERIFY` / `AIDEV-TODO` anchors for non-obvious decisions. No comments that restate code.
- Docs kept minimal: README (this), DEVELOPER.md (dev loop, commands Emmanuel runs), BURNLOG.md.

### Acceptance Criteria

Run by Emmanuel on a clean Mac with Docker running. Criteria 2–5, 7–10 apply to both modes.

- [ ] 1. `make up` (lite) is ready in under 2 min after the image is built; `make up MODE=full` in under 10 min on first run (estimates; record actuals in BURNLOG). Self-test passes in both.
- [ ] 2. A DAG saved from the page appears in the DAG list within 30 s.
- [ ] 3. A triggered run shows live task states; logs are readable for a succeeded and a failed task after their pods are gone.
- [ ] 4. A DAG with a syntax or import error shows as an import error in the page.
- [ ] 5. Grafana opens with `dagbox-overview` populated after one run, no manual import.
- [ ] 6. (full) `make up MODE=full EXECUTOR=celery` on an existing cluster switches executors; existing DAGs still run.
- [ ] 7. `git status` after `make up` shows no new tracked files; no secret in the repo (`git grep -i password` returns only docs and variable names).
- [ ] 8. A DAG importing a package listed in `requirements.txt` runs successfully after `make up`.
- [ ] 9. Ports are not reachable from another machine on the LAN (`nc -z <mac-lan-ip> 8080` from a second host fails).
- [ ] 10. Create, edit, and delete a connection and a variable from the page; a DAG reads both at run time; the connection password never appears in the page or browser storage.
- [ ] 11. (lite) `fan_out_mapping` shows overlapping task run times, proving parallel execution.
- [ ] 12. Switching modes (`make down && make up MODE=full`) keeps the DAG folder; the same `dagbox-overview` dashboard shows data in both modes.
- [ ] 13. Editing `core.max_active_tasks_per_dag` in the page, then `make apply`, changes the running value; the page shows `ok` without a reload; `make rollback` restores it.
- [ ] 14. A locked key, a secret-like key, and a misspelled key are each rejected by the page and by `make plan`.
- [ ] 15. A deliberately broken setting (for example an invalid executor value forced through `helm`) ends with `rolled_back` and a working Airflow.

Agent self-check: `make check` passes; no `AIDEV-TODO` left in shipped paths; every Open Question either resolved in BURNLOG or still marked `AIDEV-VERIFY`.

---

## v2: Control Agent (not in v1 scope)

Replaces the copy-paste step with one-click apply. It wraps the v1 `make` targets, so v1 work carries over unchanged.

| Aspect | Design |
|--------|--------|
| Run | `uvx --from git+https://github.com/ejoliet/dagbox dagbox-agent` (Python, packaged for `uvx`) |
| Bind | `127.0.0.1:<DAGBOX_AGENT_PORT>` only |
| Endpoints | `GET /status`, `POST /plan`, `POST /apply`, `POST /rollback`, `GET /events` (SSE stream of command output) |
| Execution | Calls `make plan|apply|rollback` from the repo directory. No other commands. No user-supplied arguments except a snapshot id matching `^[0-9TZ:-]+$` |
| Auth | Random token printed at start; page sends it as `Authorization: Bearer`. Token is memory-only in the page |
| Browser-attack defenses | Reject any `Origin` other than `http://localhost:<DAGBOX_PAGE_PORT>`; reject `Host` other than `127.0.0.1:<port>` / `localhost:<port>` (DNS rebinding); no CORS wildcard; no cookies |
| Concurrency | One operation at a time; a second request gets 409 |
| Audit | Append-only `$DAGBOX_HOME/agent.log` with action, snapshot, result |

Gate for v2: a threat-model review of the above, and a test page on a different origin proving every endpoint rejects it.

## Next Steps

1. [ ] Agent: Phase 0 and Phase 1a (lite).
2. [ ] Emmanuel: `make up`, lite Gate-0; resolve Q9, Q11, Q12; pin lite images in `.env.example`.
3. [ ] Agent: Phases 2–3 (page), developed against lite.
4. [ ] Emmanuel: browser acceptance criteria 2–4, 10, 11 in lite.
5. [ ] Agent: Phase 1b (full).
6. [ ] Emmanuel: `make up MODE=full`, full Gate-0; resolve Q1–Q7, Q10; pin chart versions.
7. [ ] Agent: Phase 4 (dashboards from real metric names).
8. [ ] Agent: Phase 3b (config editor and `apply.sh`).
9. [ ] Emmanuel: `make config-schema`, Gate-0 step 10 in both modes; criteria 13–15.
10. [ ] Emmanuel: remaining criteria; decide Q8 (license).
11. [ ] v2 decision: control agent first, or GitDagBundle + DAG versioning, or the visual builder first.

---

## References

- Airflow public API auth and CORS: https://airflow.apache.org/docs/apache-airflow/3.3.1/security/api.html
- Airflow DAG bundles: https://airflow.apache.org/docs/apache-airflow/3.3.1/administration-and-deployment/dag-bundles.html
- Simple auth manager: https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html
- Airflow Helm chart release notes: https://airflow.apache.org/docs/helm-chart/stable/release_notes.html
- Ruff WASM (npm): https://npmjs.com/package/@astral-sh/ruff-wasm-web
