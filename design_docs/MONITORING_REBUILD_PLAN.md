# Implementation plan: monitoring rebuild

Date: 2026-10-23, revised the same day for persisted issue state. Inputs:
`MONITORING_REVIEW_2026-10-23.md` (S1–S7), `MONITORING_REBUILD_BEHAVIOR_CHECKLIST.md` (B1–B16),
`SPEC_alert_issue_state.md`, `SPEC_alerting_active_instance.md`, `SPEC_alert_suppressions.md`.

Start point: `main` after PR 251 (`add_users`) is merged. PRs 254–262 are closed, not rebased.
One slice = one PR = one concept a reviewer can hold in their head. Every slice leaves the
monitoring deployable with all tests green. Later slices can be dropped or reordered where noted.

## Decisions (all confirmed 2026-10-23)

- D1 **One dedup mechanism.** `Issue.key` (default: `identifier`) and a per-alert
  `cooldown_minutes` where `None` means "report once per key". Replaces `unique_key`, `kind`,
  the per-alert memory sets and `reported_unique_keys`.
- D2 **Per-issue due check.** Only issues past their cooldown are notified. Issues in cooldown
  are not repeated when another issue of the same alert is due.
- D3 **Pump pressure becomes two alert classes** sharing one data loader:
  `PumpPressureIncreaseAlert` (once per detected change) and `PumpPressureHighAlert` (once per
  file). Side effect: high-pressure files are no longer throttled to one message per 2 h per
  instrument.
- D4 **`now` is an argument.** `_get_issues(status_objects, now)`; alerts never call
  `datetime.now` themselves.
- D5 **Issue state is persisted** in `alert_issue` (`SPEC_alert_issue_state.md`). Two tiers:
  DB-backed alerts are governed by records and run by the active instance only; DB-free alerts
  (db down, startup, exception, local disk, webapp health) use an in-memory cooldown and are sent
  by every instance. The "first check suppresses everything" rule is not rebuilt.
- D6 **Election decides who works**, not only who sends. Non-active instances run the DB-free
  tier only.
- D7 **Keys are explicit strings**, stable across restarts and code changes.

## Apprise readiness (decided 2026-10-23)

Messaging will later be unified via [Apprise](https://github.com/caronc/apprise): tagged
notification URLs, `notify(title, body, notify_type, tag)`. Choices below keep that a
one-module change.

- A1 One messenger facade: `send(message, tags, recipients) -> bool`. Nothing else imports
  `requests` or knows Slack or Teams.
- A2 `Message(title, body, type)`. `format_message` returns the body; `title` is the class
  attribute. `AlertTypes` keeps `INFO` and `ALERT`, later `NotifyType.INFO` / `FAILURE`.
- A3 Decorations (env prefix, host suffix, production `@channel` mention) live in the facade.
  Alerts never emit platform syntax. Whether Apprise passes `<!channel>` through is unverified.
- A4 Body markup limited to backticks and `- ` bullets, the subset Slack and Teams share.
- A5 One send per destination URL and per recipient, each with its own boolean (B2).
- A6 Config unchanged: the YAML webhook fields are valid Apprise URLs; destination names
  `ops` / `business` become the tags.

Deferred: a full notification URL per user instead of a Slack id; a per-destination length cap
(Teams: 1000 characters).

## Slices

### Slice 1: vocabulary. Issue dataclass, class names, dead code

**Value**: every later diff reads in one vocabulary; the suppressions admin page can name alerts
by class. Zero behavior change except the two small fixes.
**Scope**: `monitoring/alerts/*`, `alert_manager.py`, `main.py`, `utils.py`, tests.
**Details**:
- `Issue(identifier, details, relevant_file=None, instrument_id=None)` frozen dataclass replaces
  the `(identifier, details)` tuples (stack commit 16c6a05c is a usable template).
- `BaseAlert.name` returns `type(self).__name__`; delete `Cases` and all twelve `name` properties;
  `title` class attribute on every alert, `format_message` returns the body only, default body
  is one `- \`identifier\`: details` line per issue (B16, A2).
- `_get_issues(status_objects, now)` (D4); `ensure_utc` in `utils.py` (B12).
- Delete `SPECIAL_ALERT_SLACK_ID`, `_send_direct_message_msteams`, the unused
  `cooldown_time_minutes` parameter, `PUMP_PRESSURE_GRADIENT_TOLERANCE` (use it or drop it).
- `main.py`: `continue` after a DB connect failure (B11).
- `DiskSpaceAlert` thresholds as a dict lookup; `WebAppHealthAlert` one `RequestException` branch.
**Covers**: B11, B12, B16; review S4 (part), S5, S7.
**Tests**: existing tests adapted mechanically. New: `name` equals class name, default
`format_message`, `ensure_utc` with aware input, main loop skips check after connect failure.
**Size**: M, mostly mechanical.

### Slice 2: issue records in MongoDB

**Value**: alerts survive restarts, are never lost to cooldown or a failed send, the first alert
is immediate, and there is exactly one place that decides "notify or not". Fixes two real bugs
on `main` and removes four state mechanisms.
**Scope**: `shared/db/models.py`, `shared/db/interface.py`, `alert_manager.py`,
`base_alert.py`, stall, pump pressure, queue stop, raw file error, S3 alerts; `main.py`;
`docker-compose.yaml`, `envs/*.env`, `misc/init-mongo.sh`, `docs/deployment.md`, upgrade guide;
tests.
**Details** (per D1, D2, D5, D7; the spec is the contract):
- `AlertIssue` document and the four interface functions.
- `Issue.key: str`; `BaseAlert.cooldown_minutes: int | None`, `get_cooldown_minutes(issue)` for
  the `file_remover` exception.
- `AlertManager.check_for_issues(now)`: load open records, detect, open/notify/resolve as in the
  spec. `last_notified` written after a successful send; failure ⇒ retry next cycle.
- `AlertManager.send_once(key, message, cooldown_minutes)` with an in-memory dict for the DB-free
  tier; replaces `CustomAlert` and `send_special_alert`. Local disk and webapp health move to
  this tier.
- Remove `_reported_issues`, `_alerted_file_names`, `_alerted_subject_files`,
  `previous_raw_file_statuses`, `previous_upload_statuses`, `is_first_check`. Raw file error and
  S3 alerts query a 24 h lookback; keys `"<file>:<digest(details)>"` and `"<file>"`.
- Pump pressure keeps `kind` for one more slice, folded into its keys.
- Ops change: `MONGO_USER_ALERTING` with `alertingRole` (read all, write `alert_issue`). This is
  the first slice that writes, so the user ships here, not with the election.
**Covers**: B9, B10, B13, B14; review S1, S6; issue state spec.
**Tests**: manager tests as a table over (record state, cooldown, detected?) ⇒ (write, notify).
Failed send ⇒ unchanged record, retried. Per alert: same key across two cycles. DB-free alerts
send with the DB mocked as unreachable.
**Size**: L. The one slice that needs a careful review.

### Slice 3: destinations and recipients

**Value**: where an alert goes is a table, not an `isinstance` chain; queue stop and pump pressure
reach users directly from the users collection.
**Scope**: `alert_manager.py`, `config.py`, `recipients.py`, `messenger_clients.py`, alerts that
set `relevant_file`; tests.
**Details**:
- `ALERT_DESTINATIONS: dict[type[BaseAlert], set[Destination]]`, default `{OPS}`.
- `messenger.py` facade (A1, A3): `send(message, tags, recipients) -> bool`; replaces
  `messenger_clients.py`. Env prefix, host suffix and the production mention live here.
- `_dispatch`: channel message per distinct webhook URL, DM per issue per recipient; each send in
  its own try/except; result feeds slice 2's success flag.
- `recipients.py`: known initials and messenger ids from `get_all_users()` once per cycle;
  `extract_initials(name, known)`; no hard-coded user map.
- `relevant_file` on stall, raw file error, S3, pump pressure, queue stop.
- Queue stop: remove the DM special case; `format_message` renders issues like any alert.
**Covers**: B1, B2, B3, B4, B5, B15.
**Tests**: destination table drives sends; one failing webhook does not block the other; no
recipient ⇒ skipped and recorded; initials of a user without messenger id ⇒ no DM.
**Size**: M.

### Slice 4: suppressions from the database, admin page

**Value**: ops mute alerts at runtime without a deploy. First user-visible feature.
**Scope**: as in `SPEC_alert_suppressions.md`: `shared/keys.py`, `shared/db/models.py`,
`shared/db/interface.py`, `base_alert.py`, `alert_manager.py`, `webapp/pages_/admin.py`,
`webapp/pages_/impl/admin_utils.py`, `misc/init-mongo.sh`, upgrade guide.
**Details**: the spec is the contract. Stack commit 84d82f64 can be cherry-picked for the webapp
and shared halves; the monitoring half is re-done against slice 1's `Issue` (matching on
`issue.identifier`, `issue.instrument_id`, `str(issue.details)`). Muted issues are skipped. Delete
`suppressions.yaml`.
**Covers**: B6, B7, spec.
**Tests**: matching matrix in monitoring; validation and DataFrame tests in webapp; page tests
for add and delete.
**Size**: M. Can run in parallel with slice 3.

### Slice 5: active instance election

**Value**: one instance runs the DB-backed checks and notifies; the others idle except for the
DB-free tier. One message per alert with three instances running, and one third of the DB load.
**Scope**: as in `SPEC_alerting_active_instance.md`: `monitoring/active_instance.py`,
`main.py`, `config.py`, `misc/init-mongo.sh`, `docs/deployment.md`, upgrade guide (add the
collection to the role created in slice 2).
**Details**: cherry-pick `active_instance.py` and its tests from the stack (55b016e3); in
`main.py`, run `check_for_issues` only when active, `send_once` alerts always.
**Covers**: spec.
**Tests**: from the stack, plus main-loop test "not active ⇒ no DB-backed checks, DB-free still
run".
**Size**: S. Independent of slices 3 and 4.

### Slice 6: pump pressure, fetch/decide split

**Value**: the rule logic becomes a pure function over a list of data points; `kind` disappears;
tests stop patching `RawFile` and `augment_raw_files_with_metrics`.
**Scope**: `pump_pressure_alert.py`, tests, `ALERT_DESTINATIONS`.
**Details** (per D3):
- `load_pressure_data(instrument_ids, now) -> dict[str, list[PressureDataPoint]]` (I/O).
- `detect_increase(points, window, threshold) -> PressureChange | None` and
  `detect_high(points, threshold) -> list[PressureDataPoint]` (pure). `PressureChange` dataclass
  replaces the 6-tuple. `_is_within_tolerance` at module level, using the config constant.
- Two alert classes, both `BUSINESS`, `cooldown_minutes = None`, keys
  `"<instrument>:<younger>:<older>"` and `"<instrument>:<file>"`.
**Covers**: B8 (by construction); review S2.
**Tests**: rule tests on literal lists of data points; one integration test per class with the
loader mocked.
**Size**: M. Optional: the module works without it.

### Slice 7: queue stop, fetch/decide split

**Value**: the two rules read as two short functions; the issue is immutable; tests stop
patching `RawFile` 29 times.
**Scope**: `queue_stop_alert.py`, tests.
**Details**:
- `RecentFile(id, created_at, initials)` built once per instrument from the top-3 query.
- `detect(files: list[RecentFile], now, max_gradient, multiplier) -> QueueStopIssue | None`
  with rule A and rule B as `_stop(files, now)` and `_handoff(files)`.
- `QueueStopIssue` frozen, without sizes; `format_message` queries sizes for the stored ids
  right before rendering. `refresh_recent_files` is deleted.
**Covers**: review S2, S3.
**Tests**: rule matrix on literal `RecentFile` lists; one integration test with the query mocked.
**Size**: M. Optional.

### Slice 8: instrument stall from YAML

**Value**: `InstrumentStallAlert` can actually fire.
**Scope**: `shared/yamlsettings.py` (new key), `config.py`, `instrument_stall_alert.py`, docs.
**Details**: instrument ids from the notification settings; empty ⇒ alert disabled, logged once
at startup.
**Tests**: configured ids ⇒ checked; none ⇒ no query.
**Size**: S. Any time after slice 1.

### Slice 9: resolved notifications

**Value**: ops learn when an issue is gone without checking. Closes the oldest TODO in `main.py`.
**Scope**: `base_alert.py` (`notify_resolved: bool` per class), `alert_manager.py`, tests.
**Details**: when a record is resolved and the class opts in, send one message
"Resolved: <title> `<identifier>`" to the class's channel destinations. Opt in for stale status,
disk space, health check, pile-ups; not for once-only alerts.
**Tests**: resolved record ⇒ one message, once.
**Size**: S. After slice 2.

### Slice 10: per-cycle snapshot (optional)

**Value**: all alerts are pure functions over one `Snapshot` (status objects, recent raw files,
users, pressure data) loaded once per cycle; per-instrument queries collapse into one each.
**Scope**: `alert_manager.py`, a new `snapshot.py`, every alert's `_get_issues`.
**Details**: do only after slices 6 and 7, which already separate fetch from decide per alert.
**Size**: M. Follow-up, not required.

Follow-up outside this plan: an ops dashboard page in the webapp reading `alert_issue`
(`OPS_DASHBOARD_IDEAS.md`).

## Status (2026-10-23)

Slices 1 to 8 are implemented, one branch per slice stacked on the previous one
(`alerts_refactor_I` to `alerts_refactor_IX`, on top of `add_users`). Slice 9 was implemented
and then dropped; slice 10 is not done.

Deviations from the text above:

- The issue records are notification memory only: no `resolved_at`, no resolve step, no
  reopen. An issue that disappears and comes back keeps its cooldown. Decided after review:
  the lifecycle caused flapping alerts and only served a dashboard that does not exist.
- Slice 9 (resolved notifications) is dropped for the same reason.
- `SPECIAL_ALERT_SLACK_ID` was kept (slice 1): an earlier decision explicitly kept it.
- `db_timeout` is folded into `db_connection_error` (slice 2).
- After review, slice III also retries failed sends per issue, treats permanent Slack errors
  as done, chunks channel messages and gives up loudly after repeated failures; slice IV
  rejects empty suppression fields and applies suppressions to the DB-free tier.
- The pump pressure data is loaded once per class, i.e. twice per cycle (slice 6); the
  queue-stop alert and the recipients each read the users once per cycle (slice 3).
- `monitoring/tests` are not run in CI (`.github/workflows/branch-checks.yaml` runs `shared`,
  `webapp`, `airflow_src` only). Not changed here; worth one line in the workflow.

## Dependency graph

```
1 ─► 2 ─► 3 ─► 6, 7 ─► 10 (optional)
     ├─► 4 (parallel with 3)
     ├─► 5 (parallel with 3, 4)
     └─► 9
1 ─► 8 (any time)
```

## Checklist coverage

| Slice | Checklist items |
|---|---|
| 1 | B11, B12, B16 |
| 2 | B9, B10, B13, B14 |
| 3 | B1, B2, B3, B4, B5, B15 |
| 4 | B6, B7 |
| 6 | B8 |

## Not in this plan

- `alerts/config.py` import-time `sys.exit` (review 1.6). Separate small PR if wanted.
- `pytz` vs `datetime.UTC`: 26 files elsewhere use `pytz`; not a monitoring concern.
- Indexes on `RawFile.updated_at_` / `created_at`: pre-existing, check separately.
