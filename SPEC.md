# SPEC: Queue-End User Alert

## 1. Objective

Notify the operator who acquired the last measurements on an instrument once their measurement queue appears to have ended, so they can either start the next batch or investigate a stalled instrument.

- "Queue ended" is detected when the time since the most recent file for a known operator exceeds **3 × the gradient length** observed between their two latest consecutive files on the same instrument.
- The alert covers two cases the user explicitly called out:
  - a) The next sample is being measured by someone else (happy case, the user is just done).
  - b) No more samples are queued, or the queue errored out and stopped.
- Target users: instrument operators identified by their initials in raw file names (e.g. `MaSc`). Optionally a single "supervisor" Slack ID is CC'd on every alert.

## 2. Detection logic (acceptance criteria)

The alert runs inside the existing `monitoring/` service, polled every `CHECK_INTERVAL_SECONDS = 60` seconds by `AlertManager`.

### 2.1 Per-instrument trigger condition

For every instrument with at least two raw files in the DB:

1. Fetch the **two newest `RawFile` records** for that instrument, ordered by `created_at` desc. No initials filter at this step.
2. Extract the initials token from both file names by matching the pattern `_<initials>_` (case-sensitive, must match a key in `INSTRUMENT_USER_SLACK_IDS`).
3. If **both files share the same initials** AND those initials are present in `INSTRUMENT_USER_SLACK_IDS` → continue. Otherwise skip this instrument.
4. Compute `gradient_length = newest.created_at - second_newest.created_at` (as a `timedelta`).
5. If `now - newest.created_at > 3 × gradient_length` → emit an alert for `(instrument_id, newest_file_id, initials)`.

### 2.2 Edge cases

- Fewer than 2 raw files on the instrument → skip.
- Two newest files have different initials → skip (this is the "next sample by someone else" case; user has implicitly been told they're done because someone took over).
- Initials present but not in the mapping → skip (no one to notify).
- `gradient_length <= 0` (clock skew, simultaneous timestamps) → skip with a warning log.
- File name contains multiple `_<initials>_` matches → use the **first match** that exists in the mapping.

### 2.3 Cooldown / dedup

- Alert at most **once per `(instrument_id, newest_file_id)`**, matching the in-memory tracker pattern from `InstrumentStallAlert._alerted_file_names`.
- The tracker is reset for an instrument once a newer file appears.
- No time-based cooldown on top of this (the new-file-only mechanism already prevents re-fires for the same stall).

### 2.4 Message content

For each alert, send a Slack DM containing:

- A header line naming the instrument, the inferred gradient length (minutes), and the elapsed pause (minutes).
- The **three newest raw files on the instrument** (any initials, ordered newest → oldest) with for each:
  - `original_name`
  - size (formatted as GB / MB)
  - precursor count (looked up from `Metrics` via `augment_raw_files_with_metrics`, key `"precursors"` — may be `None` if no metrics yet; render as `n/a`).
- Plain Slack `mrkdwn` formatting consistent with existing alerts (backticks for identifiers, no @channel).

Recipients per alert:

- The Slack user ID mapped from the matched initials (always).
- `SPECIAL_ALERT_SLACK_ID` (if configured) — additionally as a CC, same message body.

## 3. Project structure / files to add or change

```
monitoring/
  alerts/
    config.py                          # add constants (see 3.1)
    queue_end_alert.py                 # NEW: QueueEndAlert(BaseAlert)
    __init__.py                        # export QueueEndAlert
  alert_manager.py                     # add QueueEndAlert() to self.alerts list
  messenger_clients.py                 # add send_slack_dm() using bot token + chat.postMessage
  tests/
    alerts/
      test_queue_end_alert.py          # NEW: unit tests (see §5)
shared/
  keys.py                              # add EnvVars.SLACK_BOT_TOKEN
```

No DB schema changes. No new YAML keys (mapping lives in Python per the chosen config approach).

### 3.1 New constants in `monitoring/alerts/config.py`

```python
# Queue-end alert configuration
QUEUE_END_THRESHOLD_MULTIPLIER = 3            # pause > N × gradient_length triggers alert
INSTRUMENT_USER_SLACK_IDS: dict[str, str] = { # initials -> Slack user ID
    # "MaSc": "U231231231231",
}
SPECIAL_ALERT_SLACK_ID: str | None = None     # optional CC'd Slack user ID
```

And in `Cases`:

```python
QUEUE_END = "queue_end"
```

### 3.2 New Slack DM transport in `messenger_clients.py`

- Read `SLACK_BOT_TOKEN` from `os.environ[EnvVars.SLACK_BOT_TOKEN]` once at import.
- `send_slack_dm(user_id: str, message: str) -> None` posts to `https://slack.com/api/chat.postMessage` with `Authorization: Bearer <token>` and `{"channel": user_id, "text": message}`.
- Honour the existing hostname/env prefix conventions (`[<env>]` prefix in non-production, `🚨` emoji for alert type).
- Raise on `response.json()["ok"] is False`; let `BaseAlert`'s caller log.

### 3.3 `QueueEndAlert` shape

- Inherits `BaseAlert`.
- `name → Cases.QUEUE_END`.
- `_get_issues(status_objects)` returns a list of `(identifier, payload)` where:
  - `identifier = f"{instrument_id}:{initials}"` (used by the cooldown machinery).
  - `payload` is a dataclass `QueueEndIssue(instrument_id, initials, slack_user_id, gradient_length, pause, recent_files)` where `recent_files` is a list of three `(name, size_bytes, precursors)` tuples.
- `format_message(issues)` builds the multi-issue message text.
- Override `get_webhook_url()` is **not** used — this alert sends via DM, not webhook. Instead override `_handle_issues`-time delivery by giving the alert its own send path. **Implementation choice:** add a `send(self, issues)` hook to `BaseAlert` defaulting to the webhook flow, override it in `QueueEndAlert` to loop over recipients and call `send_slack_dm`. Adjust `AlertManager._handle_issues` to call `alert.send(message, issues)` (passing the already-formatted message) instead of `send_message(...)`. This is the smallest deviation from the existing pattern; if you prefer to keep `AlertManager` untouched, branch on `isinstance(alert, QueueEndAlert)` inside the manager — but the override is cleaner.

## 4. Code style

- Follow the surrounding conventions in `monitoring/alerts/`:
  - Constants in UPPERCASE in `config.py`. No magic numbers/strings in `queue_end_alert.py`.
  - Docstrings on the class and `name` property; method docstrings only where non-trivial. No WHAT comments.
  - Imports at the top of the module.
  - Timezone-aware datetimes with `pytz.UTC` (match `instrument_stall_alert.py`).
- Use the existing `augment_raw_files_with_metrics(raw_files, ["precursors"])` helper from `shared.db.interface` for precursor counts; do not query `Metrics` directly.
- The initials-extraction regex lives as a module-level constant (e.g. `INITIALS_PATTERN = re.compile(r"_([A-Za-z]{2,8})_")`).
- Logging at INFO for "checking…/found N issues" and DEBUG for the per-instrument skips.

## 5. Testing strategy

Mirror `monitoring/tests/alerts/test_pump_pressure_alert.py` and `test_instrument_stall_alert.py`. Unit tests only — no integration tests against a real Mongo or Slack.

Required tests in `monitoring/tests/alerts/test_queue_end_alert.py`:

1. `test_name_returns_queue_end_case`.
2. `test_no_alert_when_fewer_than_two_files`.
3. `test_no_alert_when_two_newest_have_different_initials`.
4. `test_no_alert_when_initials_not_in_mapping`.
5. `test_no_alert_when_pause_below_threshold` (e.g. pause = 2 × gradient_length).
6. `test_alert_fires_when_pause_exceeds_threshold` (3.5 × gradient_length).
7. `test_alert_includes_three_newest_files_with_size_and_precursors`.
8. `test_alert_includes_user_slack_id_and_special_id_when_configured`.
9. `test_alert_skips_special_id_when_not_configured`.
10. `test_cooldown_no_repeat_alert_for_same_newest_file_id`.
11. `test_cooldown_releases_when_newer_file_appears`.
12. `test_handles_gradient_length_zero_or_negative_gracefully` (skip + log).
13. `test_initials_pattern_matches_underscored_token_only` (e.g. `MaScfoo` should NOT match).

Mocks:
- Patch `monitoring.alerts.queue_end_alert.RawFile` for query control.
- Patch `monitoring.alerts.queue_end_alert.augment_raw_files_with_metrics` to inject precursors.
- Patch `monitoring.alerts.queue_end_alert.INSTRUMENT_USER_SLACK_IDS` and `SPECIAL_ALERT_SLACK_ID` per test.
- Freeze `datetime.now` (use `pytz.UTC`).

For the `send_slack_dm` change in `messenger_clients.py`, add a focused test that asserts the correct payload / endpoint / auth header is built; mock `requests.post`.

Run with the existing project commands (per CLAUDE.md):

```bash
python -m pytest monitoring/tests/alerts/test_queue_end_alert.py
pre-commit run --all-files
```

## 6. Boundaries

### Always do
- Keep the new alert wholly inside `monitoring/`; no DAG or webapp changes.
- Reuse `BaseAlert`, `AlertManager`, suppression machinery, and `augment_raw_files_with_metrics`.
- Initials matching is case-sensitive and must match a key in `INSTRUMENT_USER_SLACK_IDS` exactly.
- All datetimes UTC-aware via `pytz`.
- Read the Slack bot token from `os.environ[EnvVars.SLACK_BOT_TOKEN]`; log and skip sends (don't crash the monitor) if it's missing.

### Ask first
- Any change to the initials regex beyond `_<initials>_` with `[A-Za-z]{2,8}`.
- Adding a YAML-driven mapping (current decision is Python dict in `config.py`).
- Adding any time-based cooldown layer on top of the per-file dedup.
- Changing the "three newest files on the instrument" rule (e.g. filtering by initials).
- Whether to also alert when the two newest files have **different** initials (currently: skip, by design).
- Touching unrelated files (formatters, lints, other alerts) while implementing this.

### Never do
- Do not store the initials → Slack ID mapping in the Mongo DB.
- Do not introduce a new polling loop or thread; the alert runs inside `AlertManager.check_for_issues`.
- Do not @channel or @here from this alert; DMs only (plus optional CC ID).
- Do not call Slack APIs at module-import time; only when an alert fires.
- Do not remove or modify any existing TODO comments in `monitoring/`.
- Do not add backwards-compatibility shims for the existing webhook-only `_handle_issues` path beyond what's needed to support per-alert delivery overrides.

## 7. Commands

Existing project commands apply unchanged (see CLAUDE.md):

```bash
python -m pytest                    # run unit tests
pytest --cov .                      # run with coverage
pre-commit run --all-files          # formatting, linting, type-checking
```

Environment for local manual testing:

```bash
export SLACK_BOT_TOKEN=xoxb-...     # bot must have chat:write scope
# Edit INSTRUMENT_USER_SLACK_IDS in monitoring/alerts/config.py
# Run the monitor: python monitoring/main.py
```
