# Write validation design

Status: proposal. Analysis at commit 87937c8c (2026-11-03).

Goal: one place for the validation of writes, so that a write from the MCP server or REST
API is checked exactly like one from the webapp.

Builds on `DB_INTERFACE_DESIGN.md`: requires its steps 5.1, 5.3 and 5.6 (per-aggregate
modules, domain types and errors, settings identified by `unique_name`). Section 4.8 there lists what the DB
refactoring leaves in place for this one.

## 1. Validation today

Write validation is spread over three layers. Only the middle one applies to every client.

| Rule | Where | Applies to |
|---|---|---|
| Project id: 4-16 chars, `[A-Z0-9]`, not all digits, or `_FALLBACK` | `projects.py:431` `_check_project_id` | webapp only |
| Project id: length 4-16 | `models.py` `Project.id` | any `save()` |
| Project id of a deleted project not reusable | `interface.py` `add_project` | all |
| Archived project cannot be deleted | `interface.py` `delete_project` | all |
| Settings: no `..`, absolute paths, trailing `\`, forbidden chars in file names and `software` | `settings.py:480` via `shared/validation.py` | webapp only |
| Settings: runner engine supports software type | `webapp/service/settings_validation.py` | webapp only |
| Settings: `config_params` has only known placeholders, is injection-safe after dummy substitution | `settings.py:491` via `shared/config_params.py` | webapp only |
| Settings: `slurm_mem` `^\d+[KMGT]$`, `slurm_time` `HH:MM:SS` | `settings.py:498-505` inline regexes | webapp only |
| Settings: alphadia needs fasta or speclib | `settings.py:508` | webapp only |
| Settings: name `^[a-zA-Z0-9_]+$`, max lengths, required fields | `models.py` `Settings` | any `save()` |
| Settings: version increment, unique (name, version) | `interface.py` `create_settings`, Mongo index | all |
| Settings: archived cannot be deleted, assigned cannot be deleted | `interface.py` `delete_settings` | all |
| Assignment: project active, settings not archived, default scope normalization, no duplicate file filters | `interface.py` `assign_settings_to_project` | all |
| Assignment: `*` not in excluded scopes, scopes not overlapping excluded scopes | nowhere (TODO at interface.py:331) | none |
| Assignment: `_FALLBACK` only by admin | `projects.py:60` `_check_assignment_allowed` | webapp only (authorization) |
| User: email regex, initials `^[a-zA-Z]{2,8}$` | `models.py` `User` | `save()` only, bypassed by `set_user_status` (`update_one`) |
| User: email lower-cased, unique; initials unique | `interface.py` `add_user`, `update_user` | all |
| User: strip initials, empty messenger id to None | `webapp/service/user_input.py` | webapp only |
| Suppression: fields required, comment required, not all wildcards | `webapp/pages_/impl/admin_utils.py` `validate_suppression` | webapp only |
| Suppression: min/max lengths | `models.py` `AlertSuppression` | `save()` |
| Empty strings to None | `webapp/service/utils.py` `empty_to_none` at every call | webapp only |

Observations:
- Thirteen rules are webapp-only. A settings entry created through another client would skip
  the injection checks of `shared/validation.py`.
- Rules in `models.py` run only on `save()`, not on `update()`/`update_one()`, and surface as
  `mongoengine.ValidationError`.
- The pure checks (`shared/validation.py`, `shared/config_params.py`) are already in `shared/`
  and provider-free; only their call sites are webapp-only.
- Field limits are duplicated between the Mongo schema and the webapp forms, and not equal:
  `fasta_file_name` is `max_length=128` in models.py:443 but `max_chars=64` in
  settings_utils.py:51; same for `speclib_file_name`, `config_file_name`, `software`.

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

### 2.1 Input types: `shared/domain/<aggregate>.py`

One pydantic model per user-triggered write. Pydantic is already a dependency
(`shared/runners.py`). It holds every rule of section 1 that needs neither the DB nor the
yaml: field limits and patterns, strip and empty-to-None, the pure checks of
`shared/validation.py` and `shared/config_params.py` as field validators, cross-field rules
as model validators.

```python
# shared/domain/settings.py
class NewSettings(BaseModel):
    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    name: str = Field(max_length=64, pattern=SETTINGS_NAME_REGEX)
    software_type: Literal[*SoftwareTypes.get_values()]
    software: str = Field(max_length=128)
    runner_name: str = Field(max_length=64)
    metrics_type: str = Field(max_length=128)
    description: str | None = Field(default=None, max_length=512)
    fasta_file_name: str | None = Field(default=None, max_length=128)
    speclib_file_name: str | None = Field(default=None, max_length=128)
    config_file_name: str | None = Field(default=None, max_length=128)
    config_params: str | None = Field(default=None, max_length=2048)
    slurm_cpus_per_task: int | None = Field(default=None, ge=1)
    slurm_mem: str | None = Field(default=None, pattern=SLURM_MEM_REGEX)
    slurm_time: str | None = Field(default=None, pattern=SLURM_TIME_REGEX)
    num_threads: int | None = Field(default=None, ge=1)
    owner_email: str | None = Field(default=None, pattern=USER_EMAIL_REGEX)

    _empty_to_none = field_validator("*", mode="before")(empty_to_none)

    @field_validator("fasta_file_name", "speclib_file_name", "config_file_name", "software")
    @classmethod
    def _safe_path(cls, value):            # check_for_malicious_content, raises ValueError(messages)
        ...

    @field_validator("config_params")
    @classmethod
    def _safe_config_params(cls, value):   # check_for_unknown_placeholders, then injection check on dummy-substituted value
        ...

    @model_validator(mode="after")
    def _alphadia_needs_input(self):       # fasta or speclib for alphadia
        ...
```

Likewise `NewProject` (id pattern, special ids), `NewUser` (email lower-cased, initials
regex), `NewAssignment` (`*` not in excluded scopes, scopes and excluded scopes disjoint,
default scope normalization), `NewSuppression` (fields required, not all wildcards).

Field limits and patterns are declared once here. The webapp forms read `max_chars` from
the field metadata (`NewSettings.model_fields["name"].metadata`), the Mongo models reuse
the regex constants. Field validators raise `ValueError` with today's hand-written messages;
pydantic's own messages cover only the declarative constraints.

The output types of `DB_INTERFACE_DESIGN.md` stay dataclasses.

### 2.2 Write operations: `shared/services/<aggregate>.py`

One function per write. It adds the checks that need the DB or the yaml, then persists:

```python
# shared/services/settings.py
def create(new: NewSettings) -> Settings:
    # NewSettings(...) already raised on field and cross-field errors
    errors = check_runner_supports_software_type(new.runner_name, new.software_type)  # yaml
    errors += _check_against_db(new)                                                   # owner exists, ...
    if errors:
        raise ValidationError(errors)
    return db.settings.create(new)                       # raises AlreadyExists on a version race

def archive(unique_name: str) -> None
def delete(unique_name: str) -> None                     # not archived, not assigned
def assign(new: NewAssignment) -> ProjectSettings        # project active, settings active, no duplicate filters
def unassign(assignment_id: str) -> None
```

Likewise `services.projects.create`, `archive`, `delete`; `services.users.create`,
`update`, `set_status`; `services.alerts.add_suppression`, `delete_suppression`.

The invariant checks that `shared.db` write functions hold today (archived project,
duplicate filters, unique initials, ...) move here. `shared.db` write functions take the
input type and do not validate. The Mongo field constraints stay as a backstop; the Mongo
layer already translates them to the domain `ValidationError`.

### 2.3 Errors

Clients construct the input type and catch both `pydantic.ValidationError` and the domain
`ValidationError`. `shared/domain/errors.py` gets `format_errors(pydantic_error) -> list[str]`
rendering each entry as `<field>: <message>`, so the webapp keeps its "Found N input
validation errors" toast and the MCP server returns the same list.

What this gives the MCP server: a `create_settings` tool is
`services.settings.create(NewSettings(**args))` with the two `except` clauses returning
`{"errors": [...]}`, and the LLM sees the same messages a webapp user sees.

### 2.4 What stays in the clients

- UI parsing: splitting a comma-separated filter field, the upload checkbox, bulk user row
  parsing. The MCP server passes lists directly.
- Authorization: `_check_assignment_allowed` (fallback project admin-only) is a webapp
  session rule; the MCP server and REST API need their own. See 4.2.

### 2.5 Enforcement

In addition to the ruff bans of `DB_INTERFACE_DESIGN.md` 4.7: `shared.db.projects.create`,
`shared.db.settings.create`, `shared.db.settings.assign`, `shared.db.users.create`,
`shared.db.alerts.add_suppression` banned outside `shared/services/**`.

## 3. Migration steps

Each step is a shippable PR; no compatibility shims are kept between steps.

3.1. `NewSettings` and `shared/services/settings.py`. Move the settings rules of section 1
out of `settings.py` (webapp), `settings_validation.py` (moves to `shared/services`),
`shared/db/settings.py`. The settings page constructs `NewSettings` and calls the service;
delete the inline regexes and the `empty_to_none` call sites there. Forms read `max_chars`
from the field metadata, the Mongo model reuses the regex constants.

3.2. Same for projects (`_check_project_id`), assignments (incl. the scope rules of the TODO),
users (`user_input.py` cleaners), suppressions (`validate_suppression`).

3.3. Enable the ruff ban (2.5).

3.4. MCP server write tools, if wanted: one service call each. Needs a write-capable DB user
for the MCP server; today it has the read-only one, and `init-mongo.sh` would need a role.

Tests: input types are tested by construction, without any mocking. Services are tested with
`shared.db` functions patched. Webapp validation tests move to `shared/tests/domain`.

## 4. Decisions to confirm

4.1. Pydantic for the input types, dataclasses for the output types (2.1). Clients catch two
error types, `pydantic.ValidationError` and the domain one, rendered by one formatter.
Alternative: the service wraps the pydantic error into the domain one, so clients catch a
single type but must then build inputs only inside the service call.

4.2. Fallback-project protection (`_FALLBACK` assignments admin-only) stays a client rule,
or becomes a service parameter `allow_fallback: bool` so that every client must state it.

4.3. Whether the MCP server gets write tools at all (3.4).
