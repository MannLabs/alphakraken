# SPEC: Queue-End User Alert

## 1. Objective

Notify the operator who acquired the last measurements on an instrument once their measurement queue appears to have ended, so they can either start the next batch or investigate a stalled instrument.

Two failure modes are detected, both per instrument:

- **Stall** — the most recent operator is still the most recent one and time since their last file exceeds **3 × the gradient length** observed between their two latest consecutive files.
- **Handoff** — a different operator has now taken over (the newest file's initials differ from the file before it). The previous operator's queue is treated as ended *unconditionally*; no 3× check is performed.

Target users: instrument operators identified by their initials in raw file names (e.g. `MaSc`). Optionally a single "supervisor" Slack ID is CC'd on every alert.

## 2. Detection logic (acceptance criteria)

The alert runs inside the existing `monitoring/` service, polled every `CHECK_INTERVAL_SECONDS = 60` seconds by `AlertManager`.

### 2.1 Per-instrument trigger conditions

For every instrument with at least two raw files in the DB:

1. Fetch the **two newest `RawFile` records** for that instrument, ordered by `created_at` desc. No initials filter at this step.
2. Extract the initials token from both file names by matching the pattern `_<initials>_` (case-sensitive). A file's initials are "mapped" if the token is a key in `INSTRUMENT_USER_SLACK_IDS`.

Then evaluate both rules independently (both can fire in one check):

**Rule A — Stall (active queue went quiet)**

- If both files share the *same* initials AND those initials are mapped:
  - `gradient_length = newest.created_at - second_newest.created_at`.
  - If `now - newest.created_at > 3 × gradient_length` → emit a **stall** alert for that user, keyed by `(instrument_id, newest_file_id, "stall")`.

**Rule B — Handoff (someone else took over)**

- If the newest and second-newest files have *different* initials AND the second-newest file's initials are mapped:
  - Emit a **handoff** alert for the second-newest file's user unconditionally — no gradient calc, no 3× check, no requirement that the user have ≥2 files in a row.
  - Keyed by `(instrument_id, newest_file_id, "handoff")`.

### 2.2 Edge cases

- Fewer than 2 raw files on the instrument → skip both rules.
- Neither file's initials are mapped → no alert.
- Rule A: `gradient_length <= 0` (clock skew, simultaneous timestamps) → skip Rule A with a warning log.
- File name contains multiple `_<initials>_` matches → use the **first match** that exists in the mapping.
- Newest file's initials are unmapped but second-newest's are mapped → Rule B still fires (the prior user is alerted; the new operator just isn't tracked).
- Both files have mapped initials but they differ → Rule B fires for the second-newest's user. Rule A does not fire (no same-initials pair).

### 2.3 Cooldown / dedup

- Alert at most **once per `(instrument_id, newest_file_id, kind)`** where `kind ∈ {"stall", "handoff"}`, matching the in-memory tracker pattern from `InstrumentStallAlert._alerted_file_names`.
- The tracker is reset for an instrument once a newer file appears (a new `newest_file_id` produces a fresh key).
- No time-based cooldown on top of this.

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

Recipients per alert:

- The Slack user ID mapped from the alerted-user's initials (always).
- `SPECIAL_ALERT_SLACK_ID` (if configured) — additionally as a CC, same message body.

## 3. Project structure / files to add or change

```
monitoring/
  alerts/
    config.py                          # add constants (see 3.1)
    queue_end_alert.py                 # NEW: QueueEndAlert(BaseAlert)
    base_alert.py                      # add `send` hook (see 3.3)
    __init__.py                        # export QueueEndAlert
  alert_manager.py                     # add QueueEndAlert() to self.alerts list; call alert.send(...)
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

No DB schema changes. The initials → Slack ID mapping lives in `monitoring/alerts/config.py` as a Python dict (per chosen config approach); the bot token is loaded via yamlsettings.

### 3.1 New constants in `monitoring/alerts/config.py`

```python
# Queue-end alert configuration
QUEUE_END_THRESHOLD_MULTIPLIER = 3            # stall: pause > N × gradient_length triggers alert
INSTRUMENT_USER_SLACK_IDS: dict[str, str] = { # initials -> Slack user ID
    # "MaSc": "U231231231231",
}
SPECIAL_ALERT_SLACK_ID: str | None = None     # optional CC'd Slack user ID

try:
    SLACK_BOT_TOKEN: str = get_notification_setting(YamlKeys.SLACK_BOT_TOKEN)
except KeyError:
    logging.warning("Slack bot token not found in config; QueueEndAlert DMs disabled.")
    SLACK_BOT_TOKEN = ""
```

And in `Cases`:

```python
QUEUE_END_STALL = "queue_end_stall"
QUEUE_END_HANDOFF = "queue_end_handoff"
```

### 3.2 New Slack DM transport in `messenger_clients.py`

- `send_slack_dm(user_id: str, message: str, token: str, message_type: str = AlertTypes.ALERT) -> None`.
- Posts to `https://slack.com/api/chat.postMessage` with `Authorization: Bearer <token>` and JSON `{"channel": user_id, "text": <prefixed_message>}`.
- Honour existing hostname / env prefix conventions (`[<env>]` prefix in non-production, `🚨` / `ℹ️` emoji prefix matching `_send_slack_message`).
- Raise on HTTP failure; raise on `response.json()["ok"] is False` (Slack returns 200 + `ok: false` for API-level errors). Let the caller log/handle.
- If `token` is empty: log a warning and return (do not raise) — keeps the monitor loop alive when the token is missing.

### 3.3 `QueueEndAlert` shape and delivery hook

- Inherits `BaseAlert`.
- Two issue kinds, distinguished by an enum or string field on the payload: `STALL` and `HANDOFF`.
- `name` property returns a fixed identifier (e.g. `Cases.QUEUE_END_STALL` is reused as the "alert name" for cooldown bookkeeping; per-issue kind lives inside the payload). Alternatively split into two alert classes — see "Open implementation choice" below.
- `_get_issues(status_objects)` returns `list[tuple[identifier, QueueEndIssue]]` where:
  - `identifier = f"{instrument_id}:{newest_file_id}:{kind}"` (drives cooldown).
  - `payload` is a dataclass `QueueEndIssue(kind, instrument_id, alerted_initials, slack_user_id, new_operator_initials | None, gradient_length | None, pause | None, recent_files)`. `recent_files` is a list of three `(name, size_bytes, precursors)` tuples (newest first).
- `format_message(issues)` dispatches on `kind` to produce stall vs handoff text.

**Delivery hook (chosen wiring):**

- Add `BaseAlert.send(self, message: str, issues: list) -> None` with a default implementation that calls `send_message(message, self.get_webhook_url())` — i.e. preserves today's webhook-only behaviour for all existing alerts.
- `QueueEndAlert.send` overrides this: for each issue, look up the recipient list (`[slack_user_id] + ([SPECIAL_ALERT_SLACK_ID] if set else [])`) and call `send_slack_dm(...)` per recipient. Deduplicate the recipient list per issue.
- `AlertManager._handle_issues` is changed once: replace `send_message(message, webhook_url)` with `alert.send(message, issues)`.

Open implementation choice (call out in PR description): one `QueueEndAlert` class with kind-discriminated payloads vs. two classes (`QueueStallAlert` + `QueueHandoffAlert`). One class is simpler to wire and shares the recipient lookup / file-listing helpers; two classes are tidier semantically. Default to **one class**; revisit during code review if the kind-branching gets ugly.

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

**Setup / wiring**
1. `test_name_returns_queue_end_case`.
2. `test_no_alert_when_fewer_than_two_files`.
3. `test_no_alert_when_no_initials_in_either_file_mapped`.
4. `test_initials_pattern_matches_underscored_token_only` (e.g. `MaScfoo` should NOT match).

**Rule A — stall (same initials, 3× threshold)**
5. `test_stall_no_alert_when_pause_below_threshold` (pause = 2 × gradient_length).
6. `test_stall_alert_fires_when_pause_exceeds_threshold` (3.5 × gradient_length).
7. `test_stall_handles_gradient_length_zero_or_negative_gracefully` (skip + log).
8. `test_stall_includes_three_newest_files_with_size_and_precursors`.
9. `test_stall_cooldown_no_repeat_alert_for_same_newest_file_id`.
10. `test_stall_cooldown_releases_when_newer_file_appears`.

**Rule B — handoff (different initials, unconditional)**
11. `test_handoff_fires_when_newest_two_have_different_initials_and_prior_is_mapped`.
12. `test_handoff_does_not_require_prior_user_to_have_two_files_in_a_row` (only one file in prior run → still alert).
13. `test_handoff_skips_when_prior_user_initials_unmapped`.
14. `test_handoff_fires_even_when_new_operator_initials_unmapped` (alert goes to prior user only).
15. `test_handoff_cooldown_no_repeat_alert_for_same_newest_file_id`.

**Recipients & delivery**
16. `test_alert_includes_user_slack_id_and_special_id_when_configured`.
17. `test_alert_skips_special_id_when_not_configured`.
18. `test_recipients_deduplicated_when_user_id_equals_special_id`.
19. `test_send_called_per_recipient` (asserts QueueEndAlert.send fans out to send_slack_dm per recipient).

Mocks:
- Patch `monitoring.alerts.queue_end_alert.RawFile` for query control.
- Patch `monitoring.alerts.queue_end_alert.augment_raw_files_with_metrics` to inject precursors.
- Patch `monitoring.alerts.queue_end_alert.INSTRUMENT_USER_SLACK_IDS` and `SPECIAL_ALERT_SLACK_ID` per test.
- Patch `monitoring.alerts.queue_end_alert.send_slack_dm` (or the wider `messenger_clients.send_slack_dm`) for delivery assertions.
- Freeze `datetime.now` (use `pytz.UTC`).

For `send_slack_dm` in `messenger_clients.py`, add a focused test asserting endpoint, auth header, payload, and the `ok: false` failure path; mock `requests.post`.

For the new `BaseAlert.send` hook, add a sanity test that a vanilla `BaseAlert` subclass with no override still routes through `send_message(...)` (so existing alerts are unaffected).

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
- Preserve the existing webhook flow for every other alert — the new `BaseAlert.send` hook must default to today's behaviour.

### Ask first
- Any change to the initials regex beyond `_<initials>_` with `[A-Za-z]{2,8}`.
- Adding a YAML-driven mapping for `INSTRUMENT_USER_SLACK_IDS` (current decision is Python dict in `config.py`).
- Adding any time-based cooldown layer on top of the per-file dedup.
- Changing the "three newest files on the instrument" rule (e.g. filtering by initials).
- Whether to also alert the NEW operator on a handoff (currently: only the prior user is alerted).
- Whether the handoff case should require a 3× threshold (currently: unconditional).
- Splitting `QueueEndAlert` into separate stall and handoff classes during implementation.
- Touching unrelated files (formatters, lints, other alerts) while implementing this.

### Never do
- Do not store the initials → Slack ID mapping in the Mongo DB.
- Do not introduce a new polling loop or thread; the alert runs inside `AlertManager.check_for_issues`.
- Do not @channel or @here from this alert; DMs only (plus optional CC ID).
- Do not call Slack APIs at module-import time; only when an alert fires.
- Do not check the Slack bot token into the repo. It must come from the per-env yaml that is environment-substituted at deploy time.
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
# Put the bot token in envs/alphakraken.local.yaml under general.notifications.slack_bot_token
# (bot must have chat:write scope and have been DM-opened to each target user, or the user
#  must allow DMs from apps in their workspace).
# Edit INSTRUMENT_USER_SLACK_IDS in monitoring/alerts/config.py
# Run the monitor: python monitoring/main.py
```
