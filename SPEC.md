# Spec: REST API write parity with the webapp

## Objective

The REST API can do everything the webapp does, except admin operations, so scripts and
other services do not need the webapp.

Scope is parity, not general CRUD. The webapp has no edit for projects, and settings are
immutable versions, so there is no PATCH or PUT.

| Area | Operation | `shared.db` call | Endpoint |
|---|---|---|---|
| Projects | add | `projects.add` | `POST /projects/` |
| Projects | archive | `projects.archive` | `POST /projects/{project_id}/archive` |
| Projects | soft-delete | `projects.delete` | `DELETE /projects/{project_id}` |
| Settings | create next version | `settings.create` | `POST /settings/?archive_previous=false` |
| Settings | archive | `settings.archive` | `POST /settings/{settings_id}/archive` |
| Settings | soft-delete | `settings.delete` | `DELETE /settings/{settings_id}` |
| Assignments | assign | `project_settings.assign` | `POST /assignments/` |
| Assignments | unassign | `project_settings.unassign` | `DELETE /assignments/{assignment_id}` |
| Assignments | update to latest version | `project_settings.update_to_latest` | `POST /assignments/{assignment_id}/update_to_latest` |
| Status | read | `status.find` | `GET /status/` |
| Users | read active (owner choice) | `users.find` | `GET /users/` (email, initials only; needs key) |

Existing reads (`/raw_files/`, `/projects/`, `/settings/`) stay unchanged, except that
`SettingsAssignmentResponse` gains `id`, which unassign and update-to-latest need.

Out of scope (admin): user writes, alert suppressions, creating `_FALLBACK` or changing its
assignments. The REST API always passes `allow_fallback=False` (`projects.add` and the three
assignment functions): the API key is no admin role, and `shared/db/__init__.py` (rule 4)
requires `allow_fallback` to come from authentication, never from the request.

### Decisions (2026-11-18)

- D1. Auth: one shared API key for writes and `GET /users/`. It is read from env
  `REST_API_KEY` and sent in the header `X-API-Key`. Other reads stay open. A missing or wrong
  key gives 401. If `REST_API_KEY` is unset, every keyed request gives 401 (fails closed). Keys
  are compared with `secrets.compare_digest`.
  The key is in addition to the nginx basic auth (`misc/nginx.conf:180`), because
  `.htpasswd` is shared with the webapp users `admin` and `kraken`, and compose publishes the
  container port 8090 directly (`docker-compose.yaml:455`), which bypasses nginx.
- D2. Request bodies are the domain input models `NewProject`, `NewSettings` and
  `NewAssignment` (`extra="forbid"`), so the field rules and the OpenAPI schema come from one
  place, incl. the regex patterns (`json_schema_extra`, `2131c8a2`).
  `archive_previous` is a query parameter because `NewSettings` forbids extra fields.
  Routes call the write functions as the rules in `shared/db/__init__.py:3-11` require:
  every field passed by name, never `**body`; one test per write endpoint sends a misspelled
  key and expects 422.
  Consequence: `NewAssignment` has no defaults, so a REST client must send all four lists
  (`scopes`, `excluded_scopes`, `raw_file_id_filter`, `raw_file_id_exclude_filter`).
  `assign` requires only `scopes` (`0c3381e3`) and defaults the other three to empty.
  Not in the schema: the project id rule (custom validator, `_FALLBACK` exempt), the email
  format of `owner_email`, and the allowed values of `software_type` and `metrics_type`.
- D3. The business rules stay in `shared.db`. Routes only call it and map its errors:
  `InvalidInputError` → 422, `NotFoundError` → 404, `AlreadyExistsError` → 409.
  Since `5d6d5c84`, `NotFoundError` only means that the record addressed in the path does not
  exist (project, settings, assignment). An unknown reference in a body (owner email, the
  project or settings of an assignment) is one entry of the `InvalidInputError`, so 422.
  `AlreadyExistsError`: the project id is in use, or a concurrent create took the same
  settings version or number.
  Every 422, whether FastAPI's body validation or the domain's, has the shape
  `{"detail": ["<field>: <message>", ...]}` and uses the message format of
  `shared.domain.inputs.parse`. For that, `_message` (`shared/domain/inputs.py:40`) becomes
  public, and the `RequestValidationError` handler drops the leading `"body"` from each
  error's `loc`, so a body error reads `name: ...`, not `body.name: ...`.
- D4. Responses: create returns 201 with the created resource in the existing response
  model. Archive, delete and unassign return 204. Update-to-latest returns 200 with the
  assignment.
- D5. Mongo access: a new role `restApiRole` in `misc/init-mongo.sh` with find/insert/
  update/remove on `project`, `settings` and `project_settings` only, plus `read`. Unlike
  `webappRole`, it cannot write `user` or `alert_suppression` (admin data). A new user with
  that role, env `MONGO_USER_REST_API` / `MONGO_PASSWORD_REST_API`, in `envs/*.env`, the
  mongo init service and the `rest-api` service of `docker-compose.yaml`.
- D6. Existing deployments: `init-mongo.sh` only runs when the database is first created.
  `docs/deployment.md` gets the `mongosh` commands to create `restApiRole` and the user on a
  running instance, and the step to set `REST_API_KEY`. The section "Changes visible to REST
  API and MCP clients" of `docs/upgrade_guides/upgrade_from_v0.12.0.md` lists the new
  endpoints, the `X-API-Key` header and the new `id` of the assignments.
- D7. The known issue below stays out of scope and is recorded here only.

### Known issue exposed by this change (not fixed here)

- K2. `settings.create(archive_previous=True)` archives the latest *active* version
  (`shared/db/settings.py:120-121`). The webapp checkbox labels the latest non-deleted one.
  The REST docstring must state the active-version behaviour.

(K1, archiving or deleting `_FALLBACK`, was fixed in `4ef854d4`.)

## Tech Stack

Python 3.11, FastAPI 0.136.3, uvicorn 0.52.4, pydantic v2, MongoDB via `shared.db`.

Importing `shared.db.settings` or `shared.db.project_settings` loads
`/opt/airflow/envs/alphakraken.<ENV_NAME>.yaml` (known scopes, runners), so the REST process
needs that file also for reads. The image copies it; the other `shared.db` modules don't load it.

## Commands

```
Test (REST only): /Users/mschwoerer/miniconda3/envs/alphakraken/bin/python -m pytest rest_api
Test (CI set):    /Users/mschwoerer/miniconda3/envs/alphakraken/bin/python -m pytest shared webapp airflow_src rest_api
Lint/format:      pre-commit run --files <changed files>
Run locally:      REST_API_KEY=<key> python -m rest_api.main
```

## Project Structure

```
rest_api/main.py      → app, exception handlers (D3)
rest_api/routes.py    → endpoints; write routes depend on the API-key check
rest_api/auth.py      → API-key dependency (new)
rest_api/schemas.py   → response models (+ assignment id, status, user)
rest_api/service.py   → shared.db calls and dict building (`_to_dict`, one helper since `acf9cdc0`)
rest_api/tests/       → TestClient tests, shared.db patched
shared/domain/inputs.py → error message formatter, made public for D3
docker-compose.yaml   → REST_API_KEY env, REST API Mongo user (D5)
envs/*.env            → MONGO_USER_REST_API, MONGO_PASSWORD_REST_API
misc/init-mongo.sh    → restApiRole and its user (D5)
docs/deployment.md    → setup on existing deployments (D6)
docs/upgrade_guides/upgrade_from_v0.12.0.md → client-visible changes (D6)
```

## Code Style

Follow the existing routes. Example of a write route:

```python
@router.post(
    "/projects/",
    response_model=ProjectResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
)
def create_project(new: NewProject) -> dict[str, Any]:
    """Add an active project."""
    project_store.add(
        project_id=new.project_id,
        name=new.name,
        description=new.description,
        owner_email=new.owner_email,
        allow_fallback=False,
    )
    return get_project(new.project_id)
```

The domain errors are mapped once, in app-level exception handlers, not with a
try/except in each route.

## Testing Strategy

pytest with FastAPI `TestClient`. `shared.db` is patched at the `rest_api.service`
boundary, as the existing tests do. Per write endpoint:

- happy path: status code, response body, `shared.db` called with the right arguments
  (incl. `allow_fallback=False`)
- 401 without a key, with a wrong key, and when `REST_API_KEY` is unset
- each domain error is mapped to its status code and the D3 body shape
- an invalid body gives 422 in the D3 shape, without the `body.` prefix
- a misspelled key gives 422 (`shared/db/__init__.py:3-11`)

The `shared.db` rules are already tested in `shared/tests` and are not retested here.

## Boundaries

- Always: run the REST and CI test sets before each commit; keep rules in `shared.db`.
- Ask first: Mongo user and role changes (`misc/init-mongo.sh`, compose env); new
  dependencies; changes to `shared.db` signatures.
- Never: add admin operations; pass `allow_fallback=True`; duplicate `shared.db`
  validation in the REST layer.

## Success Criteria

- SC1. Every endpoint in the Objective table exists and appears in `/docs`.
- SC2. A write without a valid key returns 401 and does not touch the DB.
- SC3. With a valid key, each write has the same effect as the matching webapp action.
- SC4. Domain and body validation errors return the D3 shape and status codes.
- SC5. `/projects/` and `/projects/{id}` return the assignment `id`.
- SC8. The upgrade guide lists every client-visible change (D6).
- SC6. The REST container runs as the `restApiRole` user. A project write works against
  the local compose stack; a write to `user` with that user is refused by Mongo.
- SC7. The CI test command passes.

## Open Questions

None. Resolved 2026-11-18:

- Q1. A new Mongo user with a new `restApiRole`, cf. D5 and D6.
- Q2. `GET /users/` needs the API key, because it returns emails.
- Q3. Version 0.2.0. The description no longer says "Read-only".
