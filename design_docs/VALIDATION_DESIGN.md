# Write validation design

Status: built for the webapp (2026-11-11), cf. `VALIDATION_PLAN.md`. MCP server write tools are open.

Goal: one place for the validation of writes, so that a write from the MCP server or REST
API is checked exactly like one from the webapp.

Clients use `shared/db/<aggregate>.py` (API modules) and the types and errors of
`shared/domain`; `shared/db/_mongo` is the only importer of mongoengine.

The webapp is the only client that writes projects, settings, assignments, users and
suppressions. The MCP server and the REST API are read-only (`MONGO_USER_READ`).

## 1. Validation before

The state at commit c93a6b73, kept as the record of what section 2 replaced. Paths and line
numbers refer to that commit.

Write validation was spread over three layers. Only the middle one applied to every client.

| # | Rule | Where | Applies to |
|---|---|---|---|
| 1 | Project id: 4-16 chars, `[A-Z0-9]`, not all digits, or `_FALLBACK` | `webapp/pages_/projects.py:419` `_check_project_id` | webapp only |
| 2 | Project: id length 4-16, name max 64, description max 512 | `_mongo/models.py` `Project` | any `save()` |
| 3 | Project id of a deleted project not reusable | `shared/db/projects.py` `add` | all |
| 4 | Archived project cannot be deleted | `shared/db/projects.py` `delete` | all |
| 5 | Settings: no `..`, absolute paths, trailing `\`, forbidden chars in file names and `software` | `webapp/pages_/settings.py:488` via `shared/validation.py` | webapp only |
| 6 | Settings: runner engine supports software type | `webapp/service/settings_validation.py` | webapp only |
| 7 | Settings: `config_params` has only known placeholders, is injection-safe after dummy substitution | `settings.py:499` via `shared/config_params.py` | webapp only at creation; the injection check runs again per job (`processor_impl.py:294`), the placeholder check does not |
| 8 | Settings: `slurm_mem` `^\d+[KMGT]$`, `slurm_time` `HH:MM:SS` | `settings.py:509-516` inline regexes | webapp only |
| 9 | Settings: alphadia needs fasta or speclib | `settings.py:518` | webapp only |
| 10 | Settings: runner declared in the yaml, software type known, metrics type allowed for the software type | options of the selectboxes | webapp only, implied |
| 11 | Settings: name `^[a-zA-Z0-9_]+$`, max lengths, required fields | `_mongo/models.py` `Settings` | any `save()` |
| 12 | Settings: version increment, number, unique (name, version) | `shared/db/settings.py` `create`, Mongo index | all |
| 13 | Settings: archived cannot be deleted, assigned cannot be deleted | `shared/db/settings.py` `delete` | all |
| 14 | Assignment: project active, settings not archived, default scope normalization, no duplicate file filters | `shared/db/project_settings.py` `assign` | all |
| 15 | Unassignment: project active | `shared/db/project_settings.py` `unassign` | all |
| 16 | Assignment: `*` not in excluded scopes, scopes not overlapping excluded scopes | nowhere (TODO at `shared/db/project_settings.py:69`); the form only leaves `*` out of the excluded options | none |
| 17 | Assignment: scopes are `*`, a known vendor or a configured instrument | options of the multiselects (`projects.py:36`) | webapp only, implied |
| 18 | Assignment: `_FALLBACK` only by admin | `projects.py:50` `_check_assignment_allowed` | webapp only (authorization) |
| 19 | User: email regex, initials `^[a-zA-Z]{2,8}$` | `_mongo/models.py` `User` | `save()` only |
| 20 | User: email stripped and lower-cased, unique; initials unique | `shared/db/users.py` `add`, `update` | all |
| 21 | User: strip initials, empty messenger id to None | `webapp/service/user_input.py` | webapp only |
| 22 | Owner of a project or settings is an active user | owner selectbox (`webapp/service/owner.py`); the backend only requires that the user exists | webapp only, implied |
| 23 | Suppression: fields required, comment required, not all wildcards; values stripped | `webapp/pages_/impl/admin_utils.py` `validate_suppression`, `admin.py:295` | webapp only |
| 24 | Suppression: min/max lengths | `_mongo/models.py` `AlertSuppression` | `save()` |
| 25 | Empty strings to None | `webapp/service/utils.py` `empty_to_none` at every call | webapp only |

Observations:
- Thirteen rows are webapp-only, three of them (10, 17, 22) only implied by the options a
  form offers. A settings entry created through another client would fail the injection
  check only when its first job runs, and an unknown placeholder would not be reported at all.
- Rules in `_mongo/models.py` run only on `save()`, not on `update_one()`, and surface as
  the domain `ValidationError` with `<field>: <message>` entries (`_mongo/errors.py`).
- State transitions are only partly guarded, the forms hide the rest: `assign` rejects
  archived but not deleted settings; `archive` of a deleted project or settings sets it to
  inactive, which undeletes it.
- `check_runner_supports_software_type` raises `KeyError` for an unknown runner.
- Two writes are composed in the webapp out of several API calls, with no rollback:
  "update assignment to the latest version" (`projects.py:214-232`: check, unassign, assign)
  and "create settings and archive the previous version" (`settings.py:538-559`). The first
  is why `check_not_assigned_with_same_filters` is public.
- The pure checks (`shared/validation.py`, `shared/config_params.py`) are already in `shared/`
  and provider-free; only their call sites are webapp-only.
- The regex constants `SETTINGS_NAME_REGEX`, `USER_EMAIL_REGEX`, `USER_INITIALS_REGEX` live in
  `shared/db/_mongo/models.py`, which only `shared/db` may import.
- Field limits are duplicated between the Mongo schema and the webapp forms, and not equal:

  | Field | `_mongo/models.py` | Form |
  |---|---|---|
  | settings `fasta_file_name`, `speclib_file_name`, `config_file_name`, `software` | 128 | 64 (`settings_utils.py`) |
  | project `description` | 512 | 256 (`projects.py:372`) |
  | user `messenger_id` | 512 | 64 (`admin.py:84`) |

### 1.1 Read filters

Not a write, but the same unevenness: user-supplied filter values are checked against
forbidden characters only in the webapp.

| Client | Check |
|---|---|
| webapp | `webapp/service/db.py:64` `_validate_input` on raw file ids and instruments; it uses `re.match`, so only the first character is checked |
| REST API | FastAPI's type checks (`rest_api/routes.py:29`) |
| MCP server | none, names are passed through (`mcp_server/server.py:117`) |

The query is built from the typed `RawFileFilter`, and the backend escapes the name search
(`_mongo/raw_files.py:41`, mongoengine for `icontains`). So this is an inconsistency more
than a risk. Resolved by deleting the webapp check (4.6).

## 2. Design

```
webapp / mcp_server / rest_api
        |  reads and writes:  shared.db       (validate + persist)
        |  types:             shared.domain   (output types, input types, errors)
        v
shared/db/projects.py  settings.py  project_settings.py  users.py  alerts.py
        |  may import: shared.runner_checks, shared.settings_scope_resolver (yaml)
        v
shared/db/_mongo/...          (persistence, no rules)
```

Dependency direction: `shared.domain` <- `shared.db._mongo` <- `shared.db` <- clients.

The API modules are the only entry point to the database (ruff bans `shared.db._mongo`
elsewhere), so a write cannot bypass the rules and no further ban is needed.

### 2.1 Input types: `shared/domain/<aggregate>.py`

One frozen pydantic model per user-triggered write, next to the output dataclass:

| Input type | Module | Write |
|---|---|---|
| `NewProject` | `shared/domain/projects.py` | `projects.add` |
| `NewAssignment` | `shared/domain/projects.py` | `project_settings.assign` |
| `NewSettings` | `shared/domain/settings.py` | `settings.create` |
| `NewUser`, `UserUpdate` | `shared/domain/users.py` | `users.add`, `users.update` |
| `NewSuppression` | `shared/domain/alerts.py` | `alerts.add_suppression` |

An input type holds every rule that needs neither the DB nor the yaml: strip
(`str_strip_whitespace`), empty-to-None for optional strings, field limits and patterns,
the pure checks of `shared/validation.py` and `shared/config_params.py` as field
validators, cross-field rules as model validators.

Field limits are declared once here, with the values the forms had. The forms read
`max_chars` through `max_length(model, field)`. The Mongo models keep their own, larger
limits as a backstop; the regex constants are shared (`shared/domain`).

Rules with a hand-written message are field validators raising `ValueError`; the purely
declarative ones (`max_length`, `pattern`, `ge`) give pydantic's message.

`shared/domain/inputs.py` holds the helpers: `parse`, `max_length`, `empty_to_none`.

### 2.2 Write operations: `shared/db/<aggregate>.py`

A write function takes the fields as keyword arguments, builds the input type with
`parse`, adds the rules that need the DB or the yaml, and passes the input type to the
backend:

```python
# shared/db/settings.py
def create(*, archive_previous: bool = False, **fields: Any) -> Settings:
    new = parse(NewSettings, **fields)                    # field and cross-field rules
    if errors := check_runner_supports_software_type(new.runner_name, new.software_type):
        raise ValidationError(errors)                     # yaml
    if new.owner_email:
        users.require_active(new.owner_email)             # DB
    ...                                                   # next version of the name, next number
    return _backend.create(new, number=number, version=version)
```

Rules beyond the input types:

| Write | Rules |
|---|---|
| `projects.add` | id not used by a deleted project, owner active |
| `projects.archive` / `delete` | not deleted / not archived |
| `settings.create` | runner declared in the yaml and able to run the software type, owner active |
| `settings.archive` / `delete` | not deleted / not archived, not assigned |
| `project_settings.assign` | fallback project only with `allow_fallback`, scopes known (yaml), project active, settings active, no duplicate file filters |
| `project_settings.unassign` | fallback project only with `allow_fallback`, project active |
| `project_settings.update_to_latest` | a newer active version exists, then as `assign`; assigns before it unassigns |
| `users.add` / `update` | email and initials not taken |

### 2.3 Errors

Clients catch the errors of `shared/domain/errors.py` only. `parse` turns a pydantic error
into the domain `ValidationError`, one `<field>: <message>` entry per invalid field and
the bare message for a cross-field rule; this is the format `_mongo/errors.py` uses for the
Mongo field constraints.

The webapp wraps every write in `toast_result(success_msg)` (`webapp/service/utils.py`),
which shows the success toast, or the error as toast, several validation errors as a list.

### 2.4 What stays in the clients

- UI parsing: splitting a comma-separated filter field, the upload checkbox, bulk user row
  splitting.
- Authorization: whether the caller may change the fallback project's assignments is the
  client's to state (`allow_fallback`); the webapp passes `is_admin()`.

## 3. Open

MCP server write tools (`VALIDATION_PLAN.md` slice 6): one API call per tool, the errors of
2.3 returned as `{"errors": [...]}`. Needs a write-capable DB user (`webappRole` in
`misc/init-mongo.sh` fits) and, first, access control: the server listens on plain HTTP
outside the nginx basic auth and has no authentication of its own.

## 4. Decisions

4.1. Clients catch one error type; pydantic is not visible to them (2.3).

4.2. Fallback-project protection is a parameter every client must state (2.4).

4.3. The MCP server gets write tools (section 3).

4.4. The rules stay in the `shared.db` API modules, no `shared/services` (section 2). The
API/backend split thereby keeps its purpose for these aggregates; only `status`,
`slurm_jobs`, `alerting_leases` and the issue functions of `alerts` are pure pass-through
(`DB_INTERFACE_RETROSPECTIVE.md` 1.1, 2.4).

4.5. Where forms and Mongo models differed, the form's limit is the declared one (2.1).

4.6. Read filters (1.1): the webapp check was deleted; the typed filter and the escaping of
the backend apply to all clients.
