# SPEC: Queue-End User Alert

## 1. Objective

Notify the operator who acquired the last measurements on an instrument once their measurement queue appears to have ended, so they can either start the next batch or investigate a stalled instrument.

Two modes are detected, both per instrument:

- **Stall** (failure) — the newest two files share the same initials and time since their last file exceeds **3 × the gradient length** observed between their two latest consecutive files.
- **Handoff** (happy case) — a different operator has now taken over (the newest file's initials differ from the file before it). The previous operator's queue is treated as ended *unconditionally*; no 3× check is performed.

Gradient length is estimated from the gap between the two newest files. Real LC gradients on these instruments are never longer than ~2 hours; if the gap exceeds `MAX_GRADIENT_LENGTH_HOURS` (= 2 h), the newest file is treated as the start of a fresh queue and neither rule fires for that pair.

In both cases the DM goes only to the **prior** operator (the user whose queue ended). The new operator is never notified by this alert. Optionally a single "supervisor" Slack ID is CC'd on every alert.

Target users: instrument operators identified by their initials in raw file names (e.g. `MaSc`).

## 2. Detection logic (acceptance criteria)

The alert runs inside the existing `monitoring/` service, polled every `CHECK_INTERVAL_SECONDS = 60` seconds by `AlertManager`.

### 2.1 Per-instrument trigger conditions

For every instrument with at least two raw files in the DB:

1. Fetch the **two newest `RawFile` records** for that instrument, ordered by `created_at` desc. No initials filter at this step.
2. Extract the initials token from both file names by matching the pattern `_<initials>_` (case-sensitive). A file's initials are "mapped" if the token is a key in `INSTRUMENT_USER_SLACK_IDS`.
3. Compute `gap = newest.created_at - second_newest.created_at`. If `gap > MAX_GRADIENT_LENGTH_HOURS` → treat the newest file as the start of a fresh queue and skip both rules for this instrument.

Then evaluate both rules independently (both can fire in one check):

**Rule A — Stall (active queue went quiet)**

- If both files share the *same* initials AND those initials are mapped:
  - `gradient_length = gap` (already capped at `MAX_GRADIENT_LENGTH_HOURS` by step 3).
  - If `now - newest.created_at > QUEUE_END_THRESHOLD_MULTIPLIER × gradient_length` → emit a **stall** alert for that user, keyed by `(instrument_id, newest_file_id, "stall")`. Alerted user = the user from both files (same initials).

**Rule B — Handoff (someone else took over)**

- If the newest and second-newest files have *different* initials AND the second-newest file's initials are mapped:
  - Emit a **handoff** alert for the second-newest file's user unconditionally — no gradient calc, no 3× check, no requirement that the user have ≥2 files in a row.
  - Keyed by `(instrument_id, newest_file_id, "handoff")`. Alerted user = the second-newest file's user (the prior operator). The new operator is never notified.

### 2.2 Edge cases

- Fewer than 2 raw files on the instrument → skip both rules.
- Neither file's initials are mapped → no alert.
- `gap > MAX_GRADIENT_LENGTH_HOURS` (2 h) → skip both rules (newest treated as start of a new queue).
- Rule A: `gap <= 0` (clock skew, simultaneous timestamps) → skip Rule A with a warning log.
- File name contains multiple `_<initials>_` matches → use the **first match** that exists in the mapping.
- Newest file's initials are unmapped but second-newest's are mapped → Rule B still fires (the prior user is alerted; the new operator just isn't tracked).
- Both files have mapped initials but they differ → Rule B fires for the second-newest's user. Rule A does not fire (no same-initials pair).

### 2.3 Cooldown / dedup

- Alert at most **once per `(instrument_id, newest_file_id, kind)`** where `kind ∈ {"stall", "handoff"}`, matching the in-memory tracker pattern from `InstrumentStallAlert._alerted_file_names`. This is the **only** time-based protection; there is no minimum-pause threshold and no per-user/per-instrument time window on top of it. Each new newest file becomes eligible to fire once.
- The tracker is reset for an instrument once a newer file appears (a new `newest_file_id` produces a fresh key).
- **Handoff suppression after stall** — when a stall alert fires for some `file_id` (while that file is newest), record `file_id` in a "stall-already-fired" set. Later, when that file becomes the second-newest after a handoff and Rule B would fire for it, **skip** the handoff alert. The user already heard about it.

### 2.4 Message content

For each alert, send a Slack DM containing:

- A header line that depends on the kind:
  - **Stall:** instrument id, the inferred gradient length (minutes), the elapsed pause since the last file (minutes).
  - **Handoff:** instrument id, the new operator's initials (so the recipient knows who took over), timestamp of the takeover.
- The **three newest raw files on the instrument** (any initials, ordered newest → oldest). For each:
  - `original_name`
  - size (formatted as GB / MB)
  - precursor count (looked up via `augment_raw_files_with_metrics`, key `"precursors"` — may be `None` if no metrics yet; render as `n/a`).
- Plain Slack `mrkdwn` formatting consistent with existing alerts (backticks for identifiers, no @channel).

Recipients per alert (per-issue, never mixed across issues):

- **Stall:** the Slack user ID mapped from the shared initials of files 1 and 2 (the still-active operator whose queue went quiet).
- **Handoff:** the Slack user ID mapped from the **second-newest** file's initials (the prior operator). The new operator (newest file) is **never** notified by this alert.
- `SPECIAL_ALERT_SLACK_ID` (if configured) — additionally CC'd on every alert, same message body. Dedup if the special ID equals the alerted user's ID.
- Each DM contains only that recipient's own issue; messages are not bundled across users.

## 3. Project structure / files to add or change

```
monitoring/
  alerts/
    config.py                          # add constants (see 3.1)
    queue_end_alert.py                 # NEW: QueueEndAlert(BaseAlert)
    __init__.py                        # export QueueEndAlert
  alert_manager.py                     # add QueueEndAlert() to self.alerts; isinstance branch in _handle_issues
  messenger_clients.py                 # add send_slack_dm() using bot token + chat.postMessage
  tests/
    alerts/
      test_queue_end_alert.py          # NEW: unit tests (see §5)
    test_messenger_clients.py          # NEW or extended: cover send_slack_dm
shared/
  yamlsettings.py                      # add YamlKeys.SLACK_BOT_TOKEN = "slack_bot_token"
envs/
  alphakraken.<env>.yaml               # add general.notifications.slack_bot_token (per-env)
```

No DB schema changes. `base_alert.py` is **not** touched. The initials → Slack ID mapping lives in `monitoring/alerts/config.py` as a Python dict; the bot token is loaded via yamlsettings.

### 3.1 New constants in `monitoring/alerts/config.py`

```python
# Queue-end alert configuration
QUEUE_END_THRESHOLD_MULTIPLIER = 3             # stall: pause > N × gradient_length triggers alert
MAX_GRADIENT_LENGTH_HOURS = 2                  # gap larger than this => new queue, no alert
INSTRUMENT_USER_SLACK_IDS: dict[str, str] = {  # initials -> Slack user ID
    # "MaSc": "U231231231231",
}
SPECIAL_ALERT_SLACK_ID: str | None = None      # optional CC'd Slack user ID

try:
    SLACK_BOT_TOKEN: str = get_notification_setting(YamlKeys.SLACK_BOT_TOKEN)
except KeyError:
    logging.warning("Slack bot token not found in config; QueueEndAlert DMs disabled.")
    SLACK_BOT_TOKEN = ""
```

And in `Cases`:

```python
QUEUE_END = "queue_end"   # single name covers both stall and handoff kinds
```

### 3.2 New Slack DM transport in `messenger_clients.py`

- `send_slack_dm(user_id: str, message: str, token: str, message_type: str = AlertTypes.ALERT) -> None`.
- Posts to `https://slack.com/api/chat.postMessage` with `Authorization: Bearer <token>` and JSON `{"channel": user_id, "text": <prefixed_message>}`.
- Honour existing hostname / env prefix conventions (`[<env>]` prefix in non-production, `🚨` / `ℹ️` emoji prefix matching `_send_slack_message`).
- Raise on HTTP failure; raise on `response.json()["ok"] is False` (Slack returns 200 + `ok: false` for API-level errors). Let the caller log/handle.
- If `token` is empty: log a warning and return (do not raise) — keeps the monitor loop alive when the token is missing.

### 3.3 `QueueEndAlert` shape and delivery wiring

- Inherits `BaseAlert`.
- Single class. `name` property returns `Cases.QUEUE_END`. The "kind" (stall vs handoff) is an internal field on the issue payload, not a separate `Cases` entry.
- `_get_issues(status_objects)` returns `list[tuple[identifier, QueueEndIssue]]` where:
  - `identifier = f"{instrument_id}:{newest_file_id}:{kind}"` (drives cooldown / dedup, including the "handoff suppressed after stall" rule via the stall-fired set).
  - `payload` is a dataclass `QueueEndIssue(kind, instrument_id, alerted_initials, slack_user_id, new_operator_initials | None, gradient_length | None, pause | None, recent_files)`. `recent_files` is a list of three `(name, size_bytes, precursors)` tuples (newest first).
- `format_message` is not used in the standard way (no shared multi-issue message). Instead the alert exposes `dispatch(issues) -> None` which formats and sends a separate DM per issue/recipient.
- `QueueEndAlert.dispatch(issues)` does:
  - For each issue, build the message text (stall or handoff template) and the recipient list: `[slack_user_id] + ([SPECIAL_ALERT_SLACK_ID] if SPECIAL_ALERT_SLACK_ID else [])`, deduplicated.
  - Call `send_slack_dm(recipient, message, SLACK_BOT_TOKEN)` per recipient.

**Wiring into `AlertManager._handle_issues`** (minimal, per-reviewer simplification):

- Keep `BaseAlert` untouched.
- Inside `AlertManager._handle_issues`, after deciding to fire, add a one-line `isinstance` branch:
  ```python
  if isinstance(alert, QueueEndAlert):
      alert.dispatch(issues)
  else:
      send_message(message, alert.get_webhook_url())
  ```
- No `send` hook on `BaseAlert`; no changes to any other alert.

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

**Setup / common**
1. `test_name_returns_queue_end_case` (`Cases.QUEUE_END`).
2. `test_no_alert_when_fewer_than_two_files`.
3. `test_no_alert_when_no_initials_in_either_file_mapped`.
4. `test_initials_pattern_matches_underscored_token_only` (e.g. `MaScfoo` should NOT match).
5. `test_no_alert_when_gap_exceeds_max_gradient_length_hours` (gap = 3 h → skip both rules; newest treated as start of new queue).

**Rule A — stall (same initials, 3× threshold, gap ≤ 2 h)**
6. `test_stall_no_alert_when_pause_below_threshold` (pause = 2 × gradient_length).
7. `test_stall_alert_fires_when_pause_exceeds_threshold` (3.5 × gradient_length, gap = 30 min).
8. `test_stall_handles_gradient_length_zero_or_negative_gracefully` (skip + log).
9. `test_stall_includes_three_newest_files_with_size_and_precursors`.
10. `test_stall_cooldown_no_repeat_alert_for_same_newest_file_id`.
11. `test_stall_cooldown_releases_when_newer_file_appears`.

**Rule B — handoff (different initials, unconditional, gap ≤ 2 h)**
12. `test_handoff_fires_when_newest_two_have_different_initials_and_prior_is_mapped`.
13. `test_handoff_does_not_require_prior_user_to_have_two_files_in_a_row` (only one file in prior run → still alert).
14. `test_handoff_skips_when_prior_user_initials_unmapped`.
15. `test_handoff_fires_even_when_new_operator_initials_unmapped` (alert goes to prior user only).
16. `test_handoff_cooldown_no_repeat_alert_for_same_newest_file_id`.
17. `test_handoff_suppressed_after_stall_fired_for_same_file` — fire a stall on file F (while F is newest), then on a later check F is now second-newest and a different-initials file is newest; Rule B must **not** fire.

**Recipients & delivery**
18. `test_stall_recipient_is_shared_initials_user`.
19. `test_handoff_recipient_is_prior_file_user_not_new_operator`.
20. `test_special_id_cc_when_configured`.
21. `test_no_special_id_cc_when_not_configured`.
22. `test_recipients_deduplicated_when_user_id_equals_special_id`.
23. `test_dispatch_sends_separate_dm_per_recipient_per_issue` (asserts QueueEndAlert.dispatch fans out send_slack_dm; messages are not bundled across users).
24. `test_alert_manager_dispatches_queueendalert_via_isinstance_branch` (lives in `test_alert_manager.py` if it exists, else in this file).

Mocks:
- Patch `monitoring.alerts.queue_end_alert.RawFile` for query control.
- Patch `monitoring.alerts.queue_end_alert.augment_raw_files_with_metrics` to inject precursors.
- Patch `monitoring.alerts.queue_end_alert.INSTRUMENT_USER_SLACK_IDS`, `SPECIAL_ALERT_SLACK_ID`, `MAX_GRADIENT_LENGTH_HOURS`, `QUEUE_END_THRESHOLD_MULTIPLIER` per test.
- Patch `monitoring.alerts.queue_end_alert.send_slack_dm` (or `messenger_clients.send_slack_dm`) for delivery assertions.
- Freeze `datetime.now` (use `pytz.UTC`).

For `send_slack_dm` in `messenger_clients.py`, add a focused test asserting endpoint, auth header, payload, and the `ok: false` failure path; mock `requests.post`. Add a test that `send_slack_dm` no-ops with a warning when `token` is empty.

Run with the existing project commands (per CLAUDE.md):

```bash
python -m pytest monitoring/tests/alerts/test_queue_end_alert.py
pre-commit run --all-files
```

## 6. Boundaries

### Always do
- Keep the new alert wholly inside `monitoring/` plus the yaml/keys additions noted in §3; no DAG or webapp changes.
- Reuse `BaseAlert`, `AlertManager`, suppression machinery, and `augment_raw_files_with_metrics`.
- Initials matching is case-sensitive and must match a key in `INSTRUMENT_USER_SLACK_IDS` exactly.
- All datetimes UTC-aware via `pytz`.
- Load the Slack bot token via `get_notification_setting(YamlKeys.SLACK_BOT_TOKEN)`; if missing, log a warning at import and have `send_slack_dm` no-op rather than crashing the monitor loop.
- Leave `BaseAlert` untouched. Wire `QueueEndAlert` into `AlertManager` via a single `isinstance` branch in `_handle_issues`.

### Ask first
- Any change to the initials regex beyond `_<initials>_` with `[A-Za-z]{2,8}`.
- Adding a YAML-driven mapping for `INSTRUMENT_USER_SLACK_IDS` (current decision is Python dict in `config.py`).
- Changing `MAX_GRADIENT_LENGTH_HOURS` from 2 h or `QUEUE_END_THRESHOLD_MULTIPLIER` from 3.
- Adding any time-based cooldown layer on top of the per-file dedup, or any minimum-pause threshold.
- Changing the "three newest files on the instrument" rule (e.g. filtering by initials).
- Whether to also alert the NEW operator on a handoff (currently: only the prior user is alerted).
- Whether the handoff case should require a 3× threshold (currently: unconditional).
- Splitting `QueueEndAlert` into separate stall and handoff classes during implementation.
- Generalising the `isinstance` branch into a `BaseAlert.send` hook ("for future alerts") — defer until a second consumer actually exists.
- Touching unrelated files (formatters, lints, other alerts) while implementing this.

### Never do
- Do not store the initials → Slack ID mapping in the Mongo DB.
- Do not introduce a new polling loop or thread; the alert runs inside `AlertManager.check_for_issues`.
- Do not @channel or @here from this alert; DMs only (plus optional CC ID).
- Do not notify the new operator on a handoff. Only the prior operator (second-newest file's user) is DM'd.
- Do not call Slack APIs at module-import time; only when an alert fires.
- Do not check the Slack bot token into the repo. It must come from the per-env yaml that is environment-substituted at deploy time.
- Do not remove or modify any existing TODO comments in `monitoring/`.
- Do not modify `BaseAlert` or any other alert class as part of this change.

## 7. Commands

Existing project commands apply unchanged (see CLAUDE.md):

```bash
python -m pytest                    # run unit tests
pytest --cov .                      # run with coverage
pre-commit run --all-files          # formatting, linting, type-checking
```

Environment for local manual testing:

```bash
# Put the bot token in envs/alphakraken.local.yaml under general.notifications.slack_bot_token
# (bot must have chat:write scope and have been DM-opened to each target user, or the user
#  must allow DMs from apps in their workspace).
# Edit INSTRUMENT_USER_SLACK_IDS in monitoring/alerts/config.py
# Run the monitor: python monitoring/main.py
```
