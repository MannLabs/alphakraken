# Spec: refactoring the REST API before adding writes

Decided 2026-11-19. Runs before the write endpoints of `SPEC.md` (after T4 of `tasks/plan.md`).
The writes roughly double `rest_api/` (441 lines today); these steps remove what every new
endpoint would otherwise copy.

## Findings

| # | Finding | Where |
|---|---|---|
| F1 | Three shapes per entity: domain dataclass → `dict` (`vars()`, `None` dropped) → response model (`None` re-added as default). The `dict` step has no purpose and makes the service and HTTP disagree on absent vs `null` (found by the e2e tests). | `service.py` `_to_dict`, `RAW_FILE_EXCLUDED_KEYS` |
| F2 | Response models disagree with what is returned: `RawFileResponse` has `extra="allow"` and commented-out fields, so `collision_flag`, `status_details`, `backup_status`, `backup_display_base_path`, `s3_upload_path`, `instrument_file_status` and `finalized_at` are returned but not in the OpenAPI schema. `ProjectResponse` and `SettingsResponse` leave out `owner_email`. | `schemas.py` |
| F3 | "Not found" handled per route: the service returns `None`, the route raises 404. | `routes.py` `read_project`, `read_settings` |
| F4 | Pagination repeated: `limit`/`offset` declared 3×, three identical list models. | `routes.py`, `schemas.py` |
| F5 | The raw file filter declared 3× (route, `get_raw_files_with_metrics`, `_query_raw_files`), two `noqa: PLR0913`. | `routes.py`, `service.py` |
| F6 | One `routes.py` for all resources: every write slice would edit it. | `routes.py` |
| F7 | Writes would get pass-through wrappers in `service.py`, although the rules are in `shared.db`. | (design rule for the writes) |

## Steps

Each step is one commit and behaviour-preserving, except R2, which is additive (see "Client-visible").

### R1: domain errors → HTTP (F3)

- `main.py` registers exception handlers: `NotFoundError` → 404 (`{"detail": "<message>"}`).
  The handlers of `InvalidInputError` (422) and `AlreadyExistsError` (409) follow with the
  writes (SPEC D3).
- `get_project` and `get_settings_by_unique_name` raise `NotFoundError`, also for deleted
  records; routes drop their `None` checks.
- Acceptance: unknown and deleted ids still give 404 with the id in `detail`; no 404 code in
  routes.

### R2: response models from the domain objects (F1, F2)

- The service returns domain objects (or, for composed reads, the domain object plus the joined
  data); the response models are built and validated from their fields by one builder
  (`DomainResponse.of`, changed 2026-11-20 from `from_attributes=True`). `_to_dict` and
  `RAW_FILE_EXCLUDED_KEYS` go away.
- `RawFileResponse` declares every field it returns today, `extra="allow"` and the commented-out
  fields go away. `created_at_` stays out, as today.
- `ProjectResponse` and `SettingsResponse` gain `owner_email`.
- Field types stay as optional as today, even where the domain says otherwise:
  ❗ production has legacy settings with `runner_name`/`metrics_type` null although the domain
  `Settings` declares `str`. Tightening would make `/settings/` fail with 500 on production
  data, and the e2e seed cannot produce such documents (plan A8). A unit test serialises a
  legacy settings object.
- Every declared field is always present, `null` when unset; the docstring of the raw file
  query says so (today it says the keys are left out).
- Acceptance: e2e reads green; a new e2e test checks that each response carries exactly the
  declared fields; OpenAPI lists all of them.

### R3: one page model, one pagination dependency (F4)

- `Page[T]` (total, offset, limit, data) replaces the three list models; a `Pagination`
  dependency declares `limit` (1..1000, default 100) and `offset` (≥ 0) once.
- Acceptance: e2e reads green (incl. the limit range tests); query parameters and their
  constraints in OpenAPI unchanged.

### R4: raw file filter, routers per resource (F5, F6, F7)

- A `RawFileQuery` dependency builds the `RawFileFilter` once; the service takes it with the
  `Pagination` and the two `include_*` flags.
- `rest_api/routers/{raw_files,projects,settings}.py`, each an `APIRouter`; `main.py` includes
  them; `routes.py` goes away. The assignments router comes with the writes.
- Rule for the writes (SPEC D3): a write route calls the `shared.db` write function directly;
  `service.py` holds only composed reads (raw files with metrics, projects with assignments).
- Acceptance: e2e reads green; no `noqa: PLR0913` left in `rest_api/`.

## Client-visible (R2 only, additive)

- Raw files: the seven fields above are always present, `null` when unset (today: present only
  when set, and undocumented).
- Projects and settings: new `owner_email`.
- OpenAPI: the raw file fields are documented; the list schema names change to the generic
  page model (R3), the shape is unchanged.

These go into the upgrade guide section of SPEC SC8.

## Verification per step

- `python -m pytest rest_api` (unit tests adapted where they asserted the `dict` shapes)
- `e2e/restart.sh` + `python -m pytest e2e/rest_api` (26 read tests today), and once with the
  production sample on top (`e2e/restart.sh ../ignore/e2e_seed`)
- `pre-commit run --files <changed files>`

## Out of scope

- Renaming `created_at_` (decided 2026-11-19: names stay).
- Handling `DatabaseUnavailableError` (today a 500).
