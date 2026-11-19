# Tasks: REST API write parity

Plan: `tasks/plan.md`. Spec: `SPEC.md`. Commands (`$PY`, stack up/down, e2e) as in the plan.

Every task's verification includes, besides its own checks:
- `$PY -m pytest rest_api` green
- `$PY -m pytest e2e/rest_api` green against the running stack
- `pre-commit run --files <changed files>` clean
- `e2e/CI_SPEC.md` updated with what this task adds for CI (A7)

The stack is started and restarted by the user (Q1); a task that changes `init-mongo.sh`,
`e2e/compose.yaml` or requirements says so before its verification.

Each e2e test of a write checks the DB effect twice: over REST, and through `shared.db`
from the host.

## Phase 0: e2e foundation

- [ ] **T1: e2e stack (Mongo + hot-reloading REST API)**
  - Acceptance:
    - `up -d --build --wait` from scratch: both services healthy; `curl localhost:18090/health` → `{"status":"ok"}`.
    - Mongo on tmpfs, initialised by the mounted `misc/init-mongo.sh`; `_FALLBACK` exists (`GET /projects/_FALLBACK` → 200).
    - Editing a route docstring in `rest_api/routes.py` changes `/openapi.json` within ~5 s, with no container restart.
    - `ENV_NAME=e2e`, with `e2e/alphakraken.e2e.yaml` mounted at `/opt/airflow/envs/`: production instrument ids and types (`astral1–5`, `stellar1` thermo; `tims1/2` bruker; `zeno2` sciex), a `slurm` runner, display paths.
  - Verify: the checks above by hand, the edit then reverted; `docker compose ... ps` shows no restart.
  - Depends on: none
  - Files: `e2e/compose.yaml`, `e2e/e2e.env`, `e2e/alphakraken.e2e.yaml`, `e2e/README.md`, `e2e/CI_SPEC.md` (skeleton)
  - Size: S

- [ ] **T2: seed through `shared.db`**
  - Acceptance:
    - `e2e/seed.py` reads `raw_files.json`, `projects.json` and `settings.json` in the REST response format from `SEED_DIR` and writes them with the `shared.db` write functions. It runs as a one-shot `seed` service in the stack (REST image, readwrite user, `ENV_NAME=e2e`), which `rest-api` waits for (`service_completed_successfully`).
    - Mapping:
      - settings: created per name in ascending version order, then archived if inactive; old id → new id is kept for the assignments.
      - Legacy values the input types reject (`runner_name`/`metrics_type` null, empty `slurm_*`) are filled in: `runner_name` `slurm`, `metrics_type` from `software_type`, `slurm_*` left to the defaults. Each fill is logged.
      - projects: added (`_FALLBACK` already exists), then archived if inactive; assignments through `assign(..., allow_fallback=True)`.
      - raw files: `add`, then `update` (size, `file_info`, backup fields, `finalized_at`, `created_at`).
      - metrics: `add` with the placeholder settings `e2e_seed`, version 1.
    - `e2e/seed_synthetic/` (committed, hand-written, no production data) uses the same format and contains at least one of each case the tests use: an active and an inactive project, an inactive settings version, a settings assigned to a project, a `_FALLBACK` assignment, filter and excluded-scope assignments, a raw file with every metric type, a raw file with `file_info`. The default `SEED_DIR` is the synthetic one; the local production seed is used with `SEED_DIR=../ignore/e2e_seed`.
  - Verify: fresh `up` with each `SEED_DIR`; the seed logs show 0 failures; counts through REST match the input (projects, settings incl. inactive, raw files), less the logged skips.
  - Depends on: T1
  - Files: `e2e/seed.py`, `e2e/seed_synthetic/*.json`, `e2e/compose.yaml`, `e2e/README.md`
  - Size: M

- [ ] **T3: e2e harness and baseline tests of the existing reads**
  - Acceptance:
    - `e2e/rest_api/conftest.py` sets `MONGO_HOST/PORT/USER/PASSWORD` (readwrite) before `shared.db` is imported, and gives an `httpx.Client` on `localhost:18090` plus a `unique_id()` helper. If the stack is unreachable, the session fails with one clear message. Host-side `shared.db` calls are limited to ones that don't load the yaml (`/opt/airflow/envs` doesn't exist on the host); everything else comes from the seed (T2).
    - Baseline tests for `/health`, `/projects/`, `/projects/{id}`, `/settings/`, `/raw_files/` (filters, pagination, `include_metrics`, `include_file_info`) against the seed. They rely only on the cases guaranteed by `e2e/seed_synthetic/`, so they pass with either `SEED_DIR`.
    - The CI command does not collect `e2e/`.
  - Verify: e2e green; `$PY -m pytest shared webapp airflow_src rest_api --collect-only -q | grep e2e` is empty.
  - Depends on: T2
  - Files: `e2e/rest_api/conftest.py`, `e2e/rest_api/test_e2e_reads.py`
  - Size: S

- [ ] **T4: `restApiRole` and REST API Mongo user (D5)**
  - Acceptance:
    - `init-mongo.sh` creates `restApiRole` (find/insert/update/remove on `project`, `settings`, `project_settings`; plus `read`) and a user from `MONGO_USER_REST_API`/`MONGO_PASSWORD_REST_API`.
    - The e2e REST service runs as that user; the baseline e2e stays green.
    - e2e: connecting through `shared.db` as the REST user, `users.add(...)` is refused by Mongo (a `DatabaseError`), while `projects.add(...)` succeeds.
  - Verify: fresh `down` + `up`; e2e green.
  - Depends on: T3
  - Files: `misc/init-mongo.sh`, `e2e/compose.yaml`, `e2e/e2e.env`, `e2e/rest_api/test_e2e_mongo_role.py`
  - Size: S

### Checkpoint 0
- [ ] Fresh `up` healthy, reload works, baseline + role e2e green
- [ ] Review with human (esp. the `init-mongo.sh` diff)

## Phase 1: Projects

- [ ] **T5: API key, error mapping, `POST /projects/` (D1–D4)**
  - Acceptance:
    - `rest_api/auth.py` `require_api_key`: 401 for a missing key, a wrong key, and an unset `REST_API_KEY`; uses `secrets.compare_digest`.
    - `main.py` handlers: `InvalidInputError`→422, `NotFoundError`→404, `AlreadyExistsError`→409, `RequestValidationError`→422, all as `{"detail": [str, ...]}` without the `body.` prefix; `_message` becomes public in `shared/domain/inputs.py`.
    - `POST /projects/` (body `NewProject`, fields passed by name, `allow_fallback=False`) → 201 with the `ProjectResponse`.
  - Verify: unit tests for each status code, the misspelled key and `allow_fallback=False`. e2e: create → 201 and visible in `GET /projects/{id}`; repeat → 409; `_FALLBACK` → 422; owner email unknown → 422; misspelled key → 422; no key → 401 and the project does not exist in the DB.
  - Depends on: T4
  - Files: `rest_api/auth.py`, `rest_api/main.py`, `rest_api/routes.py`, `shared/domain/inputs.py`, `rest_api/tests/test_write_projects.py`, `e2e/rest_api/test_e2e_projects.py`
  - Size: M (6 files; the e2e test file is small)

- [ ] **T6: archive and delete a project**
  - Acceptance:
    - `POST /projects/{id}/archive` and `DELETE /projects/{id}` → 204, keyed.
    - Archived → `status` `inactive` in `GET /projects/{id}`; deleted → 404 there (`get_project` hides deleted).
    - Second archive/delete → 422; unknown id → 404; `_FALLBACK` → 422.
  - Verify: unit + e2e for each bullet.
  - Depends on: T5
  - Files: `rest_api/routes.py`, `rest_api/tests/test_write_projects.py`, `e2e/rest_api/test_e2e_projects.py`
  - Size: S

### Checkpoint 1
- [ ] Unit + e2e green; 401/404/409/422 bodies checked over HTTP
- [ ] Review with human

## Phase 2: Settings and assignments

- [ ] **T7: `POST /settings/`**
  - Acceptance:
    - Body `NewSettings`, query `archive_previous` (default false), keyed → 201 with `SettingsResponse`; a second create of the same name → version 2.
    - `archive_previous=true` archives the latest *active* version (K2, documented in the route docstring).
    - Unknown runner/software type combination → 422.
  - Verify: unit + e2e; runner and software type taken from `alphakraken.local.yaml` via `shared.yamlsettings`.
  - Depends on: T5
  - Files: `rest_api/routes.py`, `rest_api/service.py`, `rest_api/tests/test_write_settings.py`, `e2e/rest_api/test_e2e_settings.py`
  - Size: S

- [ ] **T8: archive and delete settings**
  - Acceptance:
    - `POST /settings/{id}/archive`, `DELETE /settings/{id}` → 204, keyed.
    - Delete while assigned → 422 naming the projects (uses a seeded assigned settings).
    - Second archive/delete → 422; unknown id → 404.
  - Verify: unit + e2e.
  - Depends on: T7
  - Files: `rest_api/routes.py`, `rest_api/tests/test_write_settings.py`, `e2e/rest_api/test_e2e_settings.py`
  - Size: S

- [ ] **T9: assignment `id` in responses, `POST /assignments/`**
  - Acceptance:
    - `SettingsAssignmentResponse.id` filled in `/projects/` and `/projects/{id}`.
    - `POST /assignments/` (body `NewAssignment`, all four lists required, `allow_fallback=False`) → 201 with the assignment incl. `id` and `project_id`.
    - Unknown scope, unknown project or settings, duplicate, `_FALLBACK`, archived settings → 422 each; several at once → one 422 listing all.
  - Verify: unit + e2e; the returned `id` equals the one in `GET /projects/{id}`.
  - Depends on: T7
  - Files: `rest_api/schemas.py`, `rest_api/service.py`, `rest_api/routes.py`, `rest_api/tests/test_write_assignments.py`, `e2e/rest_api/test_e2e_assignments.py`
  - Size: M

- [ ] **T10: unassign and update to latest**
  - Acceptance:
    - `DELETE /assignments/{id}` → 204; gone from `GET /projects/{id}`.
    - `POST /assignments/{id}/update_to_latest` → 200 pointing at the newest active version; no newer version → 422.
    - Unknown id → 404; `_FALLBACK` assignment → 422 (uses a seeded `_FALLBACK` assignment).
  - Verify: unit + e2e.
  - Depends on: T9
  - Files: `rest_api/routes.py`, `rest_api/tests/test_write_assignments.py`, `e2e/rest_api/test_e2e_assignments.py`
  - Size: S

### Checkpoint 2
- [ ] Unit + e2e green; CI command green
- [ ] Review with human

## Phase 3: Reads and rollout

- [ ] **T11: `GET /status/` and `GET /users/`**
  - Acceptance:
    - `GET /status/` (open) returns all `KrakenStatus` entries.
    - `GET /users/` (keyed) returns active users, `email` and `initials` only; inactive users are left out.
  - Verify: unit + e2e with status and users seeded through `shared.db`; `/users/` without a key → 401.
  - Depends on: T5
  - Files: `rest_api/schemas.py`, `rest_api/service.py`, `rest_api/routes.py`, `rest_api/tests/test_reads_status_users.py`, `e2e/rest_api/test_e2e_reads.py`
  - Size: M

- [ ] **T12: version, main compose/env, deployment docs (D5, D6, Q3)**
  - Acceptance:
    - `main.py`: version `0.2.0`, description without "Read-only".
    - `docker-compose.yaml`: `rest-api` uses `MONGO_USER_REST_API`, gets `REST_API_KEY`; `mongodb-service` passes the new user vars; `envs/local.env` and `envs/production.env` / `sandbox.env` have the entries (production as placeholders).
    - `docs/deployment.md`: `mongosh` commands for role + user on an existing DB, and the `REST_API_KEY` step.
    - Upgrade guide `upgrade_from_v0.12.0.md`: new endpoints, `X-API-Key`, assignment `id` (SC8).
  - Verify: `docker compose --env-file envs/.env-airflow --env-file envs/local.env config` lists the new vars for `rest-api`; the `mongosh` commands from the docs, run against the e2e Mongo started *without* them in `init-mongo.sh` (temporarily skip T4's block via a scratch copy), give a working REST user.
  - Depends on: T6, T8, T10, T11
  - Files: `rest_api/main.py`, `docker-compose.yaml`, `envs/*.env`, `docs/deployment.md`, `docs/upgrade_guides/upgrade_from_v0.12.0.md`
  - Size: M

### Checkpoint: Complete
- [ ] SPEC SC1–SC8 met; fresh `up` + full e2e green; CI command green
- [ ] Review with human

## Phase 4: CI

- [ ] **T13: e2e job in CI**
  - Acceptance:
    - `e2e/CI_SPEC.md` is complete: trigger, runner, services, env, wait strategy, run
      command, expected duration, artifacts on failure (compose logs), and what is excluded
      (e.g. hot reload, which CI does not need).
    - A job in `.github/workflows/branch-checks.yaml` (or its own workflow, per the spec)
      starts the stack with `--wait`, runs `pytest e2e/rest_api`, uploads the compose logs
      on failure, and always runs `down`.
    - The job fails when an e2e test fails (checked once with a deliberately broken assertion
      on a scratch branch, then reverted).
  - Verify: the job is green on the PR; the red run from the broken assertion is linked in
    the PR description.
  - Depends on: T12
  - Files: `e2e/CI_SPEC.md`, `.github/workflows/*.yaml`, possibly `e2e/compose.yaml` (CI
    override without bind mounts and reload)
  - Size: S

### Checkpoint: CI
- [ ] e2e job green on the PR
- [ ] Review with human
