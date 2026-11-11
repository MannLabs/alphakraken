# Write validation design

Status: proposal. Analysis at commit c93a6b73 (2026-11-11).

Goal: one place for the validation of writes, so that a write from the MCP server or REST
API is checked exactly like one from the webapp.

Starting point: the DB refactoring is done. Clients use `shared/db/<aggregate>.py` (API
modules) and the types and errors of `shared/domain`; `shared/db/_mongo` is the only
importer of mongoengine; settings are identified by `unique_name`. The API modules still
hold the invariant checks, each marked `# moves to shared/services, cf. VALIDATION_DESIGN.md`
(8 places).

The webapp is the only client that writes projects, settings, assignments, users and
suppressions. The MCP server and the REST API are read-only (`MONGO_USER_READ`).

## 1. Validation today

Write validation is spread over three layers. Only the middle one applies to every client.

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
than a risk. See 4.6.

## 2. Target design

```
webapp / mcp_server / rest_api
        |  writes:  shared.services   (validate + persist)
        |  reads:   shared.db
        |  types:   shared.domain     (output types, input types, errors)
        v
shared/services/projects.py  settings.py  users.py  alerts.py
        |  may import: shared.runners, shared.yamlsettings
        v
shared/db/...                 (persistence, no validation)
```

Dependency direction: `shared.domain` <- `shared.db` <- `shared.services` <- clients.

Naming: the webapp already has a package `webapp/service` and the REST API a module
`rest_api/service.py`.

### 2.1 Input types: `shared/domain/<aggregate>.py`

One pydantic model per user-triggered write. Pydantic is already a dependency
(`shared/requirements_shared.txt`, used by `shared/runners.py`). It holds every rule of
section 1 that needs neither the DB nor the yaml: field limits and patterns, strip and
empty-to-None, the pure checks of `shared/validation.py` and `shared/config_params.py` as
field validators, cross-field rules as model validators.

```python
# shared/domain/settings.py
class NewSettings(BaseModel):
    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    name: str = Field(max_length=64, pattern=SETTINGS_NAME_REGEX)
    software_type: str
    software: str = Field(max_length=128)
    runner_name: str = Field(max_length=64)
    metrics_type: str = Field(max_length=128)
    description: str | None = Field(default=None, max_length=512)
    fasta_file_name: str | None = Field(default=None, max_length=128)
    speclib_file_name: str | None = Field(default=None, max_length=128)
    config_file_name: str | None = Field(default=None, max_length=128)
    config_params: str | None = Field(default=None, max_length=2048)
    slurm_cpus_per_task: int | None = Field(default=None, ge=1)
    slurm_mem: str | None = Field(default=None, max_length=16)
    slurm_time: str | None = Field(default=None, max_length=16)
    num_threads: int | None = Field(default=None, ge=1)
    owner_email: str | None = None

    _empty_to_none = field_validator("*", mode="before")(empty_to_none)

    @field_validator("fasta_file_name", "speclib_file_name", "config_file_name", "software")
    @classmethod
    def _safe_path(cls, value):            # check_for_malicious_content, raises ValueError(messages)
        ...

    @field_validator("config_params")
    @classmethod
    def _safe_config_params(cls, value):   # check_for_unknown_placeholders, then injection check on dummy-substituted value
        ...

    @field_validator("slurm_mem")          # likewise slurm_time: regex, today's message
    @classmethod
    def _slurm_mem_format(cls, value):
        ...

    @model_validator(mode="after")
    def _software_type_rules(self):        # known type, metrics type allowed for it, fasta or speclib for alphadia
        ...
```

Likewise `NewProject` (id pattern, special ids), `NewUser` (email lower-cased, initials
regex), `NewAssignment` (`*` not in excluded scopes, scopes and excluded scopes disjoint,
default scope normalization), `NewSuppression` (fields required, not all wildcards).

Field limits and patterns are declared once here. The regex constants move from
`_mongo/models.py` to `shared/domain`, the Mongo models import them from there. The webapp
forms read `max_chars` from the field metadata (a `MaxLen` entry in
`NewSettings.model_fields["name"].metadata`, through one helper). Where the limits differ
today (section 1), one value has to be chosen, see 4.5.

Field validators raise `ValueError` with today's hand-written messages. A declarative
`pattern=` yields pydantic's "String should match pattern '...'", so rules that have a
hand-written message today (`slurm_mem`, `slurm_time`, project id) are field validators.

Behaviour changes against today: all string values are stripped (today only the empty check
strips); `empty_to_none` must pass non-strings through, as it then also sees the int fields.

The output types stay dataclasses.

### 2.2 Write operations: `shared/services/<aggregate>.py`

One function per write. It adds the checks that need the DB or the yaml, then persists:

```python
# shared/services/settings.py
def create(new: NewSettings, *, archive_previous: bool = False) -> Settings:
    # NewSettings(...) already raised on field and cross-field errors
    errors = check_runner_supports_software_type(new.runner_name, new.software_type)  # yaml, incl. unknown runner
    errors += _check_against_db(new)                                                   # owner exists and is active
    if errors:
        raise ValidationError(errors)
    version, number = ...                                 # next version of the name, next number
    return db.settings.create(new, version=version, number=number)  # AlreadyExistsError on a version race

def archive(unique_name: str) -> None                    # not deleted
def delete(unique_name: str) -> None                     # not archived, not assigned

# shared/services/project_settings.py
def assign(new: NewAssignment) -> ProjectSettings        # project active, settings active, scopes known (yaml), no duplicate filters
def unassign(assignment_id: str) -> None                 # project active
def update_to_latest(assignment_id: str) -> ProjectSettings  # today's check + unassign + assign of the webapp
```

Likewise `services.projects.create`, `archive`, `delete`; `services.users.create`,
`update`, `set_status`; `services.alerts.add_suppression`, `delete_suppression`.

The invariant checks that the `shared.db` API modules hold today (the 8 marked places)
move here. Most already read only what they need (`_backend.get_project_id`,
`_backend.assigned_project_ids`, `projects.find(ids=...)`); the user checks still load all
users and filter (`shared/db/users.py:29,47`) and get a lookup by email or initials. `shared.db` write functions take the
input type and do not validate. `check_not_assigned_with_same_filters` becomes private.
The Mongo field constraints stay as a backstop; `_mongo/errors.py` already translates them
to the domain `ValidationError`.

Names today differ from the ones above: `shared.db.projects.add`, `shared.db.users.add`.

### 2.3 Errors

Clients construct the input type and catch both `pydantic.ValidationError` and the domain
`ValidationError`. `shared/domain/errors.py` gets `format_errors(pydantic_error) -> list[str]`
rendering each entry as `<field>: <message>`, the format `_mongo/errors.py` already uses, so
the webapp keeps its "Found N input validation errors" toast and the MCP server returns the
same list.

The webapp has 13 `except Exception as e: show_error_toast(str(e))` blocks around writes;
`str()` of a pydantic error is multi-line with documentation URLs, so each of them needs
the formatter, see 3.0.3.

What this gives the MCP server: a `create_settings` tool is
`services.settings.create(NewSettings(**args))` with the two `except` clauses returning
`{"errors": [...]}`, and the LLM sees the same messages a webapp user sees.

### 2.4 What stays in the clients

- UI parsing: splitting a comma-separated filter field, the upload checkbox, bulk user row
  parsing. The MCP server passes lists directly.
- `empty_to_none` where the page uses it for display (`settings.py:425-445`).
- Authorization: `_check_assignment_allowed` (fallback project admin-only) is a webapp
  session rule; the MCP server and REST API need their own. See 4.2.

### 2.5 Enforcement

In addition to the existing ruff bans (`mongoengine`, `pymongo`, `shared.db._mongo`):
the `shared.db` write functions of the four aggregates banned outside `shared/services/**`:
`shared.db.projects.{add,archive,delete}`, `shared.db.settings.{create,archive,delete}`,
`shared.db.project_settings.{assign,unassign}`, `shared.db.users.{add,update,set_status}`,
`shared.db.alerts.{add_suppression,delete_suppression}`.

## 3. Migration steps

Each step is a shippable PR; no compatibility shims are kept between steps.

3.0. Upfront, each independent of the design decisions except where noted:

- 3.0.1. Move the regex constants from `_mongo/models.py` to `shared/domain`; add the
  project id and slurm patterns there, replacing the inline ones of the webapp.
- 3.0.2. Make each composite write one function (section 1): `update_to_latest` and
  `create(..., archive_previous=)`, for now in the `shared.db` API modules. Every write
  button of the webapp is then one call, and the public check function is gone.
- 3.0.3. One `toast_errors()` context manager in `webapp/service/utils.py` replacing the 13
  `try/except Exception/show_error_toast` blocks; it also covers the two unguarded writes
  (`admin.py:180`, `settings.py:559`). The error formatting of 2.3 then has one call site.
- 3.0.4. Choose the field limits (4.5) and align forms and models.

3.1. `NewSettings` and `shared/services/settings.py`. Move the settings rules of section 1
out of `webapp/pages_/settings.py`, `webapp/service/settings_validation.py` (moves to
`shared/services`), `shared/db/settings.py`. The settings page constructs `NewSettings` and
calls the service; delete the inline checks and the `empty_to_none` call sites of the
submit. Forms read `max_chars` from the field metadata.

3.2. Same for projects (`_check_project_id`), assignments (incl. the scope rules of the TODO),
users (`user_input.py` cleaners), suppressions (`validate_suppression`).

3.3. Enable the ruff ban (2.5).

3.4. MCP server write tools, if wanted: one service call each. Needs a write-capable DB user
for the MCP server; today it has `MONGO_USER_READ`. `webappRole` in `misc/init-mongo.sh`
already grants exactly the five collections, a second user with that role suffices.

Tests: input types are tested by construction, without any mocking. Services are tested with
`shared.db` functions patched, as the rule tests in `shared/tests/db/test_projects_and_settings.py`
and `test_users.py` already patch the backend; they move to `shared/tests/services`. The
webapp validation tests (`test_settings_validation.py`, `test_user_input.py`,
`test_admin_utils.py::test_validate_suppression*`) move to `shared/tests/domain`.

## 4. Decisions to confirm

/Decided 2026-11-11, cf. `VALIDATION_PLAN.md`: 4.1 the alternative (one error type), 4.3
yes, 4.4 the rules stay in the `shared.db` API modules, 4.5 the form's. Section 2 is to be
read accordingly: `shared/services/<aggregate>.py` is `shared/db/<aggregate>.py`, and 2.5
is dropped. Open: 4.2, 4.6.

4.1. Pydantic for the input types, dataclasses for the output types (2.1). Clients catch two
error types, `pydantic.ValidationError` and the domain one, rendered by one formatter.
Alternative: the service takes keyword arguments and builds the input type itself, wrapping
the pydantic error into the domain one. Clients then catch a single type and never see
pydantic; the forms still import the input type for `max_chars`.

4.2. Fallback-project protection (`_FALLBACK` assignments admin-only) stays a client rule,
or becomes a service parameter `allow_fallback: bool` so that every client must state it.

4.3. Whether the MCP server gets write tools at all (3.4). Without a second writing client,
the goal of this document is not yet a requirement; what remains is closing the gaps of
section 1 (rows 16, the state transitions) and giving the invariant checks a home.

4.4. A `shared/services` layer (2.2), or the rules stay in the `shared.db` API modules,
which then take the input types. The API modules are already the only entry point, so the
second option needs no ruff ban (2.5), no new package and no test moves; `shared.db` would
import `shared.runners`, and business rules stay in a package named `db`.

Interplay with the pass-through layer (`DB_INTERFACE_RETROSPECTIVE.md` 1.1, 2.4): today the
API/backend split pays off only where the API module holds rules. With `shared/services`,
the API modules of projects, settings, assignments and users lose their rules and become
pass-through as well, so a write passes three modules (service, API, backend) of which one
only forwards; the layer should then be collapsed to re-exports in the same effort. With
the rules staying in the API modules, the split keeps its purpose for these four, and only
`status`, `slurm_jobs`, `alerting_leases` and `alerts` are candidates for collapsing.

4.5. Which field limit applies where forms and models differ (section 1): the model's or
the form's.

4.6. Read filters (1.1): delete the webapp check and rely on the typed filter and the
escaping of the backend, or move the check into `RawFileFilter` so that all three clients
get it.
