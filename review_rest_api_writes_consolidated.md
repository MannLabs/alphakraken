# Consolidated review: `rest_api_writes_I` .. `rest_api_writes_IX`

- Date: 2026-11-20. Base `rest_api_writes` @ `4acb2f34`, tip `rest_api_writes_IX` @ `54ec2b5c`.
- Source: nine per-branch reviews (`review_rest_api_writes_<N>.md`), each branch against its predecessor. Deduplicated; `[N/x.y]` points to the finding in the source review.
- Line numbers are those of the branch named in the tag.
- Method: static review. Tests run only for III (`test_write_projects.py`, 18 passed). No stack started, no e2e run, `docker compose` not available.
- No critical finding in any branch.

| Branch | Content | Verdict | Important | Minor |
|---|---|---|---|---|
| I | R1–R4, T4 | request changes | 3 | 6 |
| II | T5 | request changes (small) | 5 | 7 |
| III | T6 | approve with nits | 1 | 4 |
| IV | T7 | request changes (small) | 2 | 7 |
| V | T8 | request changes (small) | 2 | 4 |
| VI | T9 | approve code | 2 | 4 |
| VII | T10 | approve | 0 | 5 |
| VIII | T11 | approve code | 1 | 5 |
| IX | T12 | approve after doc fixes | 2 | 8 |

## 1. Important

### 1.1 `e2e/` is in no commit, tasks are ticked with e2e verification
`[I/(a), II/2.1, III/2.1, IV/2.1, V/2.1, VI/2.1, VIII/2.1, IX/3.8]`
- `git log --all -- e2e` is empty. `SPEC.md:127` says "local only"; `tasks/plan.md:36,47` call `e2e/seed_synthetic/` and `e2e/e2e.env` "committed". `tasks/todo.md` lists e2e test files and a per-task `e2e/CI_SPEC.md` update for T4–T11.
- Unit tests patch the `shared.db` functions, so these have no tracked evidence:
  - T4: REST service running as the new Mongo user (`test_e2e_mongo_role.py`); R2: "exactly the declared fields".
  - T5: create → 201, repeat → 409, `_FALLBACK` → 422, unknown owner → 422.
  - T6: archived → `inactive`, deleted → 404, second archive/delete → 422.
  - T7: second create → version 2, `archive_previous` effect, unknown runner → 422.
  - T8: delete while assigned → 422, second archive/delete → 422.
  - T9: all 422 cases, returned id equals the one in `GET /projects/{id}`.
  - T11: inactive user left out against a real DB.
  - T12: main compose wiring and the documented `mongosh` commands.
- The on-disk untracked `e2e/` holds matching tests; not reviewed, not attributable to a branch.
- Fix: commit `e2e/` per branch, or make plan and todo say it stays local and untick the e2e parts.

### 1.2 `restApiRole`: `remove` on `project` and `settings` is not needed
`[I/2.2]` `misc/init-mongo.sh:86,90`; repeated in `docs/deployment.md:622` at IX.
- Archive/delete of projects and settings is `update_one(status=...)`; only `project_settings.unassign` calls `.delete()`.
- A leaked REST credential can hard-delete projects and settings. Follows SPEC D5 literally.
- Fix: drop `remove` for both in the script, SPEC D5, todo T4 and the deployment docs. Nothing is under-granted.

### 1.3 Key check runs after the body is read and JSON-parsed
`[II/2.3, VI/3.4]` `rest_api/routers/projects.py:45`, `rest_api/auth.py:16`; same pattern on all body routes.
- No key + malformed JSON → 422, not 401 (confirmed with a standalone probe on fastapi 0.136.3). Deviates from D1/SC2.
- Unauthenticated callers make the service buffer a body on the directly published port 8090. Size limit unverified.
- Fix: check the key before the body is read (ASGI middleware or router-level check), or accept in D1 and add the test.

### 1.4 OpenAPI error contract is wrong
`[II/2.2, VI/out of scope]` `rest_api/main.py:46-52`, `rest_api/routers/projects.py:42-46`; no `responses=` anywhere at IX.
- Handlers return `{"detail": [str]}`; OpenAPI still documents FastAPI's default `HTTPValidationError` for every route. 401 and 409 are undocumented.
- A client generated from `/openapi.json` fails on the 422 body.
- Fix: one error model and `responses={401, 409, 422}` on the write routes; override the default 422 schema app-wide.

### 1.5 `POST /settings/` re-reads to work around a data-layer mismatch
`[IV/2.2]` `rest_api/routers/settings.py:79-80`.
- `settings.create` returns `created_at_` with microseconds; the database stores milliseconds. The route discards the return value and reads again.
- Webapp and MCP need the same workaround. A delete between create and re-read turns a successful create into 404.
- Fix: make `_backend.create` return what is stored (`reload()` or truncate the default), then validate the returned object. Removes [3.6.2] too.

### 1.6 Deployment docs: Mongo-user step is mandatory, presented as optional
`[IX/2.1]` `docs/deployment.md:613-631`, `docs/upgrade_guides/upgrade_from_v0.12.0.md:30-33`, `docker-compose.yaml:463-464`.
- `rest-api` always connects as `MONGO_USER_REST_API`, so role and user are needed for reads too. The step sits under "Enabling the writes".
- If skipped: `/health` stays green (no DB access), every read fails on authentication.
- Fix: own section in the upgrade guide; rename the heading; mark only `REST_API_KEY` as optional; add an open read to the verification step.

### 1.7 Deployment docs: keyed examples send the API key over plain HTTP
`[IX/2.2]` `docs/deployment.md:603-607,631`.
- Both examples use `http://<ip>:8090`, the direct port; nginx (TLS + basic auth) is bypassed. Behind nginx the example fails with a TLS error or an nginx 401.
- Fix: document the nginx form as the one to use when nginx is deployed; state that the direct port is clear text.

### 1.8 T4 init script creates a user from vars the main compose passes only from T12
`[I/2.1]` `misc/init-mongo.sh:15-16,99-103`.
- Wiring is closed at IX (`[IX/checked]`), so this affects only a standalone deploy of I–VIII on a fresh DB: `createUser({user: "", pwd: ""})` fails.
- Still open at IX: the block sits before the `_FALLBACK` upsert and both `createIndex` calls. Whether mongosh aborts on the failed statement is unverified.
- Fix: move the role/user block after the seed and index statements.

### 1.9 404 `detail` texts changed in R1, declared behaviour-preserving
`[I/2.3]` `rest_api/main.py:25-27`, `rest_api/service.py:115,139-141`; `REFACTORING_SPEC.md:21,68`.
- `Project 'X' not found` → `No project X`; settings → `No settings x`, or `'nope' is not of the form <name>_v<version>`.
- The upgrade guide at IX lists `No project <id>` (`[IX/checked]`). Whether it lists the two settings texts was not checked.
- Fix: list all three in REFACTORING_SPEC and the upgrade guide.

### 1.10 Auth tests fall short of the spec's test strategy
`[III/3.2, IV/3.5, V/2.2, VI/2.2, VII/3.2]`
- SPEC requires per write endpoint: no key, wrong key, unset `REST_API_KEY`. Unset is tested only in `test_write_projects.py:83-97`; several routes test only the missing key.
- Low risk (one shared dependency), but a route with a weaker check would pass.
- Fix: one parametrised test over all non-GET routes of `app.routes` for the three cases; drop the per-file copies. Or relax the spec sentence.

### 1.11 `test_create_project` does not check which project is read back
`[II/2.4]` `rest_api/tests/test_write_projects.py:39-69`.
- The `get_project` mock is never asserted; the expected body equals the request only because the factory defaults coincide.
- Fix: assert `get_project` called once with `"P123"`; give the factory project values that differ from the request.

### 1.12 Missing unit tests for behaviour T5 defines
`[II/2.5]` `rest_api/tests/test_write_projects.py`.
- Body-level error (empty or list body); type error in a field; no key + invalid body → 401; `_FALLBACK` → 422.

## 2. Decisions to record

2.1. `[V/3.2]` Write on a deleted record → 422 with name and status; `GET` on the same id → 404. Same for projects (T6). By plan (`tasks/todo.md:152`), inconsistent, reveals deleted records. Decide once, record in SPEC D3.

2.2. `[VIII/3.3]` `/status/` and `/users/` return bare arrays; the other lists return `Page[T]`. Spec is silent. Adding pagination later is breaking.

2.3. `[VIII/3.5]` `GET /status/` is open and returns free-text `status_details` (unhealthy paths, file-remover error strings) on the direct port. Follows D1. Content of the error strings not traced.

2.4. `[VI/3.1]` `project_id` in the nested assignments of `/projects/` is not in `SPEC.md:29` / D6. The upgrade guide at IX lists it.

2.5. `[II/3.6]` Create is `add` then `get_project`: a failing read gives 500/404 although the project exists. Prescribed by the spec example.

2.6. `[VII/3.4]` `update_to_latest` → 404 for an existing assignment whose settings are gone; conflicts with D3. Base behaviour, needs a direct DB edit.

2.7. `[I/3.1]` `read` covers all of `krakendb` (also `alert_*`, `slurm_job`), mandated by D5. `find` in the three privileges is redundant with `read`.

## 3. Minor

### 3.1 Data layer and routes
3.1.1. `[VII/3.1]` Malformed `assignment_id` → 422 with a mongoengine ObjectId message, not 404 (`shared/db/_mongo/project_settings.py:44`). Recorded in `BOYSCOUT_2026-11-19.md` #4. Fix in the data layer: raise `NotFoundError`.

3.1.2. `[VIII/3.1]` `GET /users/` relies on the default `include_inactive=False` of `shared.db.users.find` (`rest_api/routers/users.py:15`); the test pins the absence of an argument. Fix: pass it explicitly.

3.1.3. `[I/3.2]` Three ways to build a response (`rest_api/schemas.py:49-53,68-78,93-103`); `ProjectResponse` / `SettingsAssignmentResponse` list fields by hand without `from_attributes` (contradicts `tasks/todo.md:84`); `RawFileResponse.of` uses an unvalidated `model_copy(update=...)`.

3.1.4. `[I/3.5]` Service signatures inconsistent: `Pagination` vs `limit`/`offset` with a literal `100` duplicating `DEFAULT_LIMIT`; `service.py` imports the HTTP-layer `dependencies`.

3.1.5. `[II/3.1]` Malformed JSON message is `"1: JSON decode error"`: the character offset reads like a field name (`rest_api/main.py:55-60`).

3.1.6. `[II/3.2]` 401 without `WWW-Authenticate` (`rest_api/auth.py:32`).

3.1.7. `[II/3.3]` Public name `message` is generic and shadowed three times in `shared/domain/inputs.py:40,42,67,78`. Suggest `error_message`.

3.1.8. `[II/3.4]` Comment in `rest_api/auth.py:25` gives a wrong reason ("fails closed instead of at startup"); the reason is that reads must work without a key.

3.1.9. `[II/3.5]` Unset `REST_API_KEY` is silent: same 401 as a wrong key, nothing logged. Suggestion only.

### 3.2 OpenAPI descriptions (route docstrings)
3.2.1. `[III/3.3, V/3.1]` `DELETE` on projects and settings omits "active only": an archived record can never be deleted; a deleted project id stays blocked (`rest_api/routers/projects.py:75`, `rest_api/routers/settings.py:99`).

3.2.2. `[IV/3.1]` `POST /settings/` omits the partial failure of `archive_previous`: 422 although the new version exists (`rest_api/routers/settings.py:55-60`).

3.2.3. `[VII/3.5]` Unassign and `update_to_latest` omit their 422 conditions (no newer version, `_FALLBACK`, archived project) (`rest_api/routers/assignments.py:42,50`).

3.2.4. `[VIII/3.2]` `free_space_gb: -1` sentinel undocumented (`rest_api/schemas.py:147`).

### 3.3 Test structure
3.3.1. `[III/3.1, V/3.4, VII/3.3]` Three `# noqa: PLR0913` in `rest_api/tests` (`test_write_projects.py:187`, `test_write_settings.py:215`, `test_write_assignments.py:187`); R4 acceptance requires none. Clean at I, broken from III.

3.3.2. `[VI/3.2, VIII/3.4]` `API_KEY`, `KEY_HEADERS` and the autouse `api_key` fixture copied into four test files. Move to `conftest.py`.

3.3.3. `[V/3.3]` The settings error-mapping tests copy the project ones and put "still assigned" onto `archive`, which cannot raise it (`test_write_settings.py:170-230`). Test the handlers once.

3.3.4. `[II/3.7]` `test_invalid_query_parameter_names_its_location` tests an app-level handler on a read inside `test_write_projects.py:141-149`.

### 3.4 Weak or missing tests
3.4.1. `[I/3.3]` Nothing ties response fields to domain fields; with `extra="allow"` gone a new domain field is silently dropped. First assertion of the OpenAPI test is tautological (`rest_api/tests/test_schemas.py:20-34`).

3.4.2. `[I/3.4]` Route test never asserts that `include_file_info` / the `include_metrics` default is passed on (`rest_api/tests/test_routes.py:132-155`).

3.4.3. `[I/3.6]` "Deleted project" unit test only tests the empty result.

3.4.4. `[IV/3.2]` Assertion hidden in fixture teardown behind a condition (`test_write_settings.py:64-66`).

3.4.5. `[IV/3.3]` `test_create_settings_fills_resource_defaults` asserts only truthiness (`test_write_settings.py:98-100`).

3.4.6. `[IV/3.4]` Happy-path payload has `slurm_cpus_per_task == num_threads` and `config_params=None`; cannot detect swapped or dropped fields (`test_write_settings.py:29-33`).

3.4.7. `[IV/3.6]` `?archive_previous=maybe` → 422 shape untested.

3.4.8. `[IV/3.7]` Magic strings `"fallback_v7"`, `"as stored"` depend on a factory default (`test_write_settings.py:66,73,74`).

3.4.9. `[VI/3.3]` Docstring promise "`scopes` needs at least one entry" has no REST-level test.

### 3.5 Deployment docs (IX)
3.5.1. `[IX/3.1]` (unverified) Missing env vars probably break every `./compose.sh` call on every host, not only the two containers the upgrade guide names (`upgrade_from_v0.12.0.md:32-33`).

3.5.2. `[IX/3.2]` "Needs no downtime" ignores that the changed `mongodb-service` definition recreates the Mongo container on the next `up` (`docs/deployment.md:615`).

3.5.3. `[IX/3.3]` Password on the command line in the `createUser` example (`docs/deployment.md:623`); ends in shell history, breaks on `"`, `$`, `\`.

3.5.4. `[IX/3.4]` `#REST_API_KEY=<api_key>` works as a key if only uncommented; `docs/deployment.md:105` omits `REST_API_KEY` and the `MONGO_PASSWORD_*` variants from the secrets to set.

3.5.5. `[IX/3.5]` `REST_API_KEY` must be identical on every host running `rest-api`; docs say "the PC" (`docs/deployment.md:618`).

3.5.6. `[IX/3.6]` `409` documented for projects only; a concurrent settings create also gives 409 (`docs/deployment.md:610`).

3.5.7. `[IX/3.7]` Demo notebook referenced for "usage examples" has no keyed or write example (`docs/deployment.md:599`).

3.5.8. `[IX/3.8]` Role definition duplicated in docs and `misc/init-mongo.sh:81-98`, no drift check.

## 4. Resolved within the stack

4.1. `[III/3.4]` "Read-only API" in the app description: fixed at IX.

4.2. `[I/2.1]` Missing compose/env wiring for the REST Mongo user: added at IX (block order remains, see 1.8).

## 5. Out of scope, pre-existing

5.1. `misc/init-mongo.sh` has no guard against unset user vars (relies on compose `:?error`). `[I]`

5.2. `webappRole` also has an unneeded `remove` on `project` / `settings`. `[I]`

5.3. `find` and `count` are two queries; `total` can disagree with the page under concurrent writes. `[I]`

5.4. `NewAssignment` lists have no maximum length (`shared/domain/projects.py:87-95`). `[VI]`

5.5. Check-then-write race in `unassign` / `update_to_latest` (accepted per comment at `shared/db/project_settings.py:65`). `[VII]`

5.6. `docs/deployment.md:512`: webapp role description omits `project_settings` and `alert_suppression`. `[IX]`

5.7. `/health` does not touch the DB. `[IX]`

## 6. Not verified

6.1. Whether mongosh continues after a failed statement in the piped init script (1.8).

6.2. Upper bound on the request body size for unauthenticated callers (1.3).

6.3. `Annotated[Pagination, Depends()]` on a dataclass, and an unparametrised `Page(...)` for `-> Page[T]` (tests exist, not run).

6.4. Production documents with unexpected types in the seven newly declared raw file fields, or explicit `null` in non-optional `KrakenStatus` fields; either would be a 500.

6.5. T12's "Verify" steps (`compose config`, `mongosh` commands against a DB without the role).

6.6. Unit tests of I, II, IV–IX.
