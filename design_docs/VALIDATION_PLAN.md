# Write validation: implementation plan

Plan for `VALIDATION_DESIGN.md`, at commit c93a6b73 (2026-11-11).

Decided (2026-11-11):
- 4.1: one error type. Write functions take keyword arguments, build the input type and
  wrap the pydantic error into the domain `ValidationError`. Clients never see pydantic.
- 4.3: the MCP server gets write tools, as the last slice.
- 4.4: the rules stay in the `shared.db` API modules. No `shared/services`, no ruff ban.
- 4.5: the form's limits are the declared ones. The Mongo models keep theirs as a backstop.

Assumed, to be confirmed:
- A1 (4.2): the assignment functions get a required keyword `allow_fallback: bool`, so every
  client states it. Webapp passes `is_admin()`, MCP server `False`.
- A2 (4.6): the webapp read filter check is deleted (R4).
- A3: the MCP write tools are `create_project`, `create_settings`, `assign_settings`. No
  users, suppressions, archive, delete, unassign.

Blocking slice 6:
- B1: the MCP server listens on port 8089 over plain HTTP outside the nginx basic auth
  (`docs/deployment.md:164`), and `mcp_server/server.py` has no authentication. Write tools
  would let anyone who reaches the port create settings, whose `software` and
  `config_params` end up in job commands. Needs an access control decision first.

## Target shape

```python
# shared/domain/inputs.py
def parse(model: type[T], **fields: Any) -> T:
    """Build the input type, raising the domain ValidationError with `<field>: <message>` entries."""

def max_length(model: type[BaseModel], field: str) -> int: ...   # for the forms' max_chars

# shared/domain/settings.py
class NewSettings(BaseModel): ...        # field limits, patterns, pure checks, cross-field rules

# shared/db/settings.py
def create(*, archive_previous: bool = False, **fields: Any) -> Settings:
    new = parse(NewSettings, **fields)   # field and cross-field rules
    ...                                  # rules needing the yaml or the DB, collected into one ValidationError
    return _backend.create(new, version=..., number=...)
```

Per aggregate: input type in `shared/domain/<aggregate>.py`, rules in
`shared/db/<aggregate>.py`, tests of the input type in `shared/tests/domain/`, tests of the
rules stay in `shared/tests/db/`.

## Refactorings

No change of validation behaviour. Each is one PR, R1 to R4 are independent of each other.

### R1: Regex constants to `shared/domain`

**Value**: input types can use them; `_mongo` may only be imported by `shared/db`.
**Scope**: `shared/domain`, `shared/db/_mongo/models.py`, `webapp/pages_`.
**Details**:
- Move `SETTINGS_NAME_REGEX` to `shared/domain/settings.py`, `USER_EMAIL_REGEX` and
  `USER_INITIALS_REGEX` to `shared/domain/users.py`. `models.py` imports them.
- Move the inline patterns of the webapp there as constants: `SLURM_MEM_REGEX`,
  `SLURM_TIME_REGEX` (`settings.py:511,515`), project id pattern, length bounds and special
  ids (`projects.py:415-427`, renaming the misnamed `ALLOWED_CHARACTERS_IN_PROJECT_ID`).
  The webapp imports them.
**Testing**: existing tests pass unchanged.

### R2: One function per composite write

**Value**: every write button is one call; the slices below replace one call each.
Fixes BOYSCOUT_2026-11-11 item 9.
**Scope**: `shared/db/project_settings.py`, `shared/db/settings.py`, `webapp/pages_`.
**Details**:
- `project_settings.update_to_latest(assignment_id) -> ProjectSettings`: assigns the latest
  active version with the scopes and filters of the given assignment, then removes the old
  one. Assign first, so a failure loses nothing. Replaces `projects.py:214-232`.
- `check_not_assigned_with_same_filters` becomes private.
- `settings.create(..., archive_previous: bool = False)`: archives the previous version
  after creating. Replaces `settings.py:558-559`.
**Testing**: new tests in `shared/tests/db/test_projects_and_settings.py` for both, incl.
"assign fails, old assignment kept"; adapt `webapp/tests/pages_/test_projects.py`,
`test_settings.py`.

### R3: `toast_errors()` in the webapp

**Value**: one place that turns an error into a toast, in place of 13 blocks. Fixes
BOYSCOUT_2026-11-11 item 8.
**Scope**: `webapp/service/utils.py`, `webapp/pages_/{admin,settings,projects}.py`.
**Details**:
- Context manager that shows `str(e)` as error toast and suppresses the exception. The
  success toast moves into the `with` block, after the write.
- Replace the 13 `try/except Exception/show_error_toast` blocks; wrap the two unguarded
  writes (`admin.py:180`, `settings.py:559` if R2 is not merged yet).
- Not the bulk user import (`admin.py:114-120`), which collects failures per row.
**Testing**: unit test of the context manager; page tests pass unchanged.

### R4: Delete the webapp read filter check (A2)

**Value**: removes a check that only tests the first character (BOYSCOUT item 14) and that
no other client has; the backend escapes the values.
**Scope**: `webapp/service/db.py:64-88`, its tests.
**Details**: delete `_validate_input` and its two calls.
**Testing**: one test in `shared/tests/db/test_raw_files.py` that an id with regex
characters is matched literally, if none exists.

## Slices

Each slice: input type, API function taking it, the webapp form switched over, tests. Each
is shippable; after each, the webapp-only rules of one aggregate apply to every client.

### Slice 1: Alert suppressions (walking skeleton)

**Value**: proves the pattern on the smallest aggregate: input type, error wrapping, toast.
Rules 23 and 24 of the design apply to all clients.
**Scope**: `shared/domain`, `shared/db/alerts.py`, `_mongo/alerts.py`, admin page.
**Details**:
- New `shared/domain/inputs.py` with `parse`, `max_length` and `empty_to_none` (passes
  non-strings through).
- `NewSuppression` in `shared/domain/alerts.py`: strip, the four match fields and the
  comment required, not all wildcards, max lengths, `created_by` empty to None. Messages
  of `validate_suppression` kept.
- `alerts.add_suppression(**fields)` parses; `_backend.add_suppression` takes the input type.
- Admin page: delete the `validate_suppression` call, the `.strip()` calls and
  `empty_to_none` (`admin.py:287-301`); delete `validate_suppression`.
- Field errors read `<field>: <message>`, e.g. `comment: Comment is required ...`.
**Testing**: `shared/tests/domain/test_inputs.py`, `test_alerts.py` (by construction, no
mocks); move the cases of `test_admin_utils.py::test_validate_suppression*`; adapt
`shared/tests/db/test_alerts.py`.

### Slice 2: Users

**Value**: rules 19 to 21 apply to all clients, and to the update as well.
**Scope**: `shared/domain/users.py`, `shared/db/users.py`, `_mongo/users.py`, admin page.
**Details**:
- `NewUser` (email stripped, lower-cased, regex; initials stripped, regex; messenger id
  empty to None, max 64) and `UserUpdate` (initials, messenger id).
- `users.add(**fields)`, `users.update(email, **fields)` parse.
- `_backend.find_by(*, email=None, initials=None)` in place of loading all users for the
  uniqueness checks (`users.py:29,47`).
- Admin page: delete `clean_initials`, `clean_messenger_id` and their uses; `max_chars`
  from `max_length`. `parse_user_row` only splits.
**Testing**: `shared/tests/domain/test_users.py`; adapt `shared/tests/db/test_users.py`,
`webapp/tests/service/test_user_input.py`.

### Slice 3: Projects

**Value**: rule 1 applies to all clients; archive no longer undeletes (BOYSCOUT item 7);
owner must be active (rule 22).
**Scope**: `shared/domain/projects.py`, `shared/db/projects.py`, `_mongo/projects.py`,
projects page.
**Details**:
- `NewProject`: id rule of `_check_project_id` with its message, name required max 64,
  description max 256 and empty to None (BOYSCOUT item 10), owner email.
- `projects.add(**fields)` parses; additionally: owner is an active user.
- `projects.archive`: reject a deleted project.
- Projects page: delete `_check_project_id` and the `empty_to_none` calls
  (`projects.py:415-441`); `max_chars` from `max_length`.
**Testing**: `shared/tests/domain/test_projects.py`; new cases in
`test_projects_and_settings.py` (inactive owner, archive of deleted); adapt `test_projects.py`.

### Slice 4: Settings

**Value**: the injection and placeholder checks (rules 5 to 10) run at creation for every
client. Largest slice.
**Scope**: `shared/domain/settings.py`, `shared/db/settings.py`, `_mongo/settings.py`,
settings page, `webapp/service/settings_validation.py`.
**Details**:
- `NewSettings` as sketched in the design 2.1, with the form's limits (file names and
  `software` 64). Field validators with today's messages: malicious content, config
  params, slurm formats. Model validator: software type known, metrics type allowed for it
  (`SOFTWARE_TYPE_TO_METRICS_TYPES`), fasta or speclib for alphadia.
- Move `check_runner_supports_software_type` with `mv` to `shared/runner_checks.py`; it
  returns an error for an unknown runner instead of raising `KeyError` (BOYSCOUT item 11).
- `settings.create(**fields)` parses; additionally: runner check, owner is an active user.
  All errors of this stage in one `ValidationError`.
- `settings.archive`: reject deleted settings.
- Settings page: delete the checks at `settings.py:487-524` and the `empty_to_none` calls
  of the submit; the upload checkbox check stays. `build_form_items` reads `max_chars`
  through `max_length`.
- The toast "Found N input validation errors" is built from `ValidationError.errors` in
  `toast_errors` (R3).
**Testing**: `shared/tests/domain/test_settings.py` covering every rule; move
`webapp/tests/service/test_settings_validation.py` to `shared/tests/`; new cases in
`test_projects_and_settings.py` (unknown runner, inactive owner, archive of deleted); adapt
`test_settings.py`.

### Slice 5: Assignments

**Value**: closes the gap of rule 16 (assignments that silently match nothing), rule 17
and BOYSCOUT item 6; fallback protection is stated by every client (A1).
**Scope**: `shared/domain/projects.py`, `shared/db/project_settings.py`,
`_mongo/project_settings.py`, projects page.
**Details**:
- `NewAssignment`: default scope normalization, `*` not in excluded scopes, scopes and
  excluded scopes disjoint, filter entries stripped and non-empty, max lengths. Resolves
  the TODO at `project_settings.py:69`.
- `project_settings.assign(*, allow_fallback: bool, **fields)` parses; additionally: scopes
  are `*`, a known vendor or a configured instrument (yaml); settings neither archived nor
  deleted; `_FALLBACK` only with `allow_fallback`.
- `unassign` and `update_to_latest` get `allow_fallback` as well.
- Projects page: `_check_assignment_allowed` is replaced by `allow_fallback=is_admin()`;
  `_split_filter` stays.
**Testing**: `shared/tests/domain/test_projects.py`; new cases in
`test_projects_and_settings.py` (unknown scope, deleted settings, fallback refused);
adapt `test_projects.py`.

### Slice 6: MCP server write tools (blocked by B1)

**Value**: settings and projects can be created through the MCP server, with the messages
a webapp user sees.
**Scope**: `mcp_server/server.py`, `docker-compose.yaml`, `misc/init-mongo.sh`,
`docs/deployment.md`, env files.
**Details**:
- New Mongo user `MONGO_USER_MCP` with `webappRole`; compose passes it to `mcp-server`.
  Existing deployments need the user created by hand, documented in `docs/deployment.md`.
- Tools `create_project`, `create_settings`, `assign_settings` (A3): one API call each,
  `ValidationError`, `NotFoundError` and `AlreadyExistsError` returned as
  `{"errors": [...]}`. `assign_settings` passes `allow_fallback=False`.
- The access control decided for B1.
**Testing**: tool tests with the `shared.db` functions patched, incl. the error shape;
manual run against the local profile.

## After the last slice

- Delete `webapp/service/utils.py::empty_to_none` if only display uses remain and they can
  use a plain truthiness check.
- Update `VALIDATION_DESIGN.md` section 2 to the built state, or delete it with this plan.
- Collapsing the pass-through API modules (`status`, `slurm_jobs`, `alerting_leases`,
  `alerts` issue functions) is independent of this plan.
