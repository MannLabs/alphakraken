# Implementation Plan: REST API write parity

Spec: `SPEC.md`. Tasks: `tasks/todo.md`.

## Overview

First an isolated end-to-end (e2e) stack: a fresh MongoDB, set up by the real `misc/init-mongo.sh`, and
the REST API with `rest_api/` and `shared/` mounted and `uvicorn --reload`. Then the spec's
endpoints in vertical slices. Each slice has unit tests (`TestClient`, `shared.db` patched)
and e2e tests (HTTP against the stack, DB effects checked through `shared.db`).

## Architecture Decisions

- A1. **Separate stack, not the `local` profile.** `e2e/compose.yaml` with its own project name
  (`kraken-e2e`) and ports (Mongo 37017, REST 18090), so it runs next to a local AlphaKraken
  stack without touching `mongodb_data_local`.
- A2. **Fresh DB on every `up`.** Mongo data lives on `tmpfs`, so `init-mongo.sh` runs on every
  start. That tests the real init script, incl. the `_FALLBACK` seed and the new `restApiRole`.
- A3. **Hot reload.** The REST service uses the existing `rest_api/Dockerfile` image, mounts
  `rest_api/`, `shared/` and `e2e/alphakraken.e2e.yaml` (`ENV_NAME=e2e`), and runs
  `uvicorn rest_api.main:app --reload --reload-dir rest_api --reload-dir shared`, with
  `WATCHFILES_FORCE_POLLING=true` because inotify events are unreliable on Docker Desktop
  bind mounts. Only changing requirements needs `--build`.
- A4. **e2e tests outside the CI paths.** They live in `e2e/rest_api/` because CI runs
  `pytest shared webapp airflow_src rest_api`. They run as their own pytest call. There is no
  `__init__.py`, and files are named `test_e2e_*.py`, to avoid basename collisions and the
  shadowing described in the pytest-path-shadowing memory.
- A5. **Tests don't share state.** There is no reset between tests: each test uses unique ids
  (project id `E2E` + 6 random chars, which fits the 4–16 limit; settings names with a random
  suffix). Host-side checks go through `shared.db` with the readwrite user. They must never
  import `shared.db.settings` or `shared.db.project_settings`: importing those loads the yaml
  from `/opt/airflow/envs`, which exists only in the containers (checked 2026-11-19; `projects`,
  `raw_files`, `users`, `status` and `metrics` import fine). Raw
  `pymongo` is never used, as the ruff ban requires.
- A8. **Seed (decided 2026-11-19).** A one-shot `seed` service writes REST-format JSON through
  the `shared.db` write functions. The committed `e2e/seed_synthetic/` is always written; the production sample
  (`ignore/e2e_seed/`, local only) can be added on top, in the same format. CI uses only the synthetic one.
  Tests rely only on cases the synthetic seed guarantees.
  Limitation: legacy settings values (`number` -1, null `runner_name`/`metrics_type`, empty
  `slurm_*`) are normalised by the write path, so reads are not tested on legacy documents
  here; the unit tests of the response models must cover them.
- A9. **e2e yaml.** `e2e/alphakraken.e2e.yaml` lists the production instruments
  (`astral1–5`, `stellar1`, `tims1/2`, `zeno2`) so the seeded scopes and instrument ids are known.
- A7. **CI spec grows along the way.** Each task appends to `e2e/CI_SPEC.md` what a CI job
  would need for it: services, env, waits, timing, flakiness seen, and what to leave out.
  T13 implements from that spec.
- A6. **Credentials.** `e2e/e2e.env` is committed and holds test-only credentials, the
  Mongo users and `REST_API_KEY`. It contains no production values.

## Dependency Graph

```
T1 e2e stack ──> T2 seed ──> T3 harness + baseline reads ──> T4 restApiRole user
                                                                   │
R1 error mapping ──> R2 response models ──> R3 page model ──> R4 filter + routers (REFACTORING_SPEC.md)
                                                                   │
                                                   ┌───────────────┘
                                                   └─> T5 auth + error mapping + POST /projects/
                                                           ├─> T6 project archive/delete
                                                           ├─> T7 settings create ─> T8 settings archive/delete
                                                           │                      └─> T9 assignment id + assign ─> T10 unassign / update_to_latest
                                                           └─> T11 status + users reads
T6..T11 ──> T12 version, compose/env, deployment docs ──> T13 CI e2e job
```

R1–R4 need only T3 (the e2e reads are their safety net); they run after T4 so that Checkpoint 0
closes the e2e foundation first. After T5, T6, T7 and T11 are independent: since R4 each
touches its own router, but they share `schemas.py`, so they still run in order.

## Task List

See `tasks/todo.md` for acceptance criteria and verification.

### Phase 0: e2e foundation
- [x] T1: e2e stack (Mongo + hot-reloading REST API)
- [x] T2: seed through `shared.db` (synthetic default, production sample locally)
- [x] T3: e2e harness and baseline tests of the existing reads
- [x] T4: `restApiRole` and REST API Mongo user

### Checkpoint 0
- [ ] `up --wait` from scratch is healthy; an edit to `rest_api/main.py` shows without restart
- [ ] baseline e2e green; the REST user cannot write `user`
- [ ] review with human

### Phase R: Refactoring (`REFACTORING_SPEC.md`)
- [x] R1: domain errors → HTTP (404 handler, no `None` returns)
- [x] R2: response models from the domain objects (all fields declared, `owner_email`)
- [x] R3: `Page[T]` and a `Pagination` dependency
- [x] R4: `RawFileQuery` dependency, routers per resource, writes call `shared.db` directly

### Checkpoint R
- [ ] unit + e2e green after each step, also with the production sample on top
- [ ] review with human (esp. R2: the client-visible additions)

### Phase 1: Projects
- [x] T5: API key, error mapping, `POST /projects/`
- [x] T6: `POST /projects/{id}/archive`, `DELETE /projects/{id}`

### Checkpoint 1
- [ ] unit + e2e green; 401/404/409/422 shapes verified over HTTP
- [ ] review with human

### Phase 2: Settings and assignments
- [x] T7: `POST /settings/` (+ `archive_previous`)
- [x] T8: `POST /settings/{id}/archive`, `DELETE /settings/{id}`
- [x] T9: assignment `id` in responses, `POST /assignments/`
- [x] T10: `DELETE /assignments/{id}`, `POST /assignments/{id}/update_to_latest`

### Checkpoint 2
- [ ] unit + e2e green, CI command green
- [ ] review with human

### Phase 3: Reads and rollout
- [ ] T11: `GET /status/`, `GET /users/` (keyed)
- [ ] T12: version 0.2.0, main compose/env, `docs/deployment.md`

### Checkpoint: Complete
- [ ] SC1–SC8 of `SPEC.md` met; full e2e from a fresh `up` green
- [ ] review with human

### Phase 4: CI
- [ ] T13: e2e job in CI, as specified in `e2e/CI_SPEC.md`

### Checkpoint: CI
- [ ] e2e job green on a PR branch
- [ ] review with human

## Commands

```
PY=/Users/mschwoerer/miniconda3/envs/alphakraken/bin/python
Stack up:    docker compose -p kraken-e2e -f e2e/compose.yaml --env-file e2e/e2e.env up -d --build --wait
Stack down:  docker compose -p kraken-e2e -f e2e/compose.yaml --env-file e2e/e2e.env down
REST logs:   docker compose -p kraken-e2e -f e2e/compose.yaml --env-file e2e/e2e.env logs -f rest-api
Unit:        $PY -m pytest rest_api
e2e:         $PY -m pytest e2e/rest_api
CI set:      $PY -m pytest shared webapp airflow_src rest_api
Lint:        pre-commit run --files <changed files>
```

Every task ends with all of: unit tests, the e2e suite against the running stack, and
`pre-commit`. Each checkpoint also does a fresh `down` + `up` (A2) before the e2e run.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Docker is not reachable from the Claude sandbox | High | You start and stop the stack (Q1). Claude runs pytest against `localhost`; if the sandbox blocks that, Claude asks before any bypass |
| Reload misses changes on bind mounts | Med | `WATCHFILES_FORCE_POLLING=true`; T1 verifies it by editing a file |
| `init-mongo.sh` changes (T4) break the production init | High | T4 runs the script from scratch in the e2e stack; the diff is reviewed at Checkpoint 0 |
| Settings create needs a runner and scopes from the yaml | Med | Dedicated `e2e/alphakraken.e2e.yaml` (A9), changed only together with the seed |
| Host `shared.db` calls that load the yaml fail (no `/opt/airflow/envs` on the host) | Med | Such writes go into the seed (A5, A8); a failing host call shows up immediately in T3 |
| Production sample changes shape on refetch (new fields, legacy values) | Low | The seed logs skips and fills; the tests don't depend on it (A8) |
| Unique-id tests leave data behind | Low | tmpfs; `down` wipes it |

## Decisions (2026-11-19)

- Q1. You start, stop and rebuild the e2e stack. Claude says when that's needed: a fresh
  `up` at each checkpoint, `--build` after a requirements change, and a restart after
  `init-mongo.sh` or compose changes, since those don't reload.
- Q2. The e2e suite goes into CI as T13; `e2e/CI_SPEC.md` is built up during T1–T12 (A7).
