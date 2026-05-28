# SPEC: Queue-End User Alert

## 1. Objective

Notify the operator who acquired the last measurements on an instrument once their measurement queue appears to have ended, so they can either start the next batch or investigate a stalled instrument.

Two modes are detected per instrument; they are **mutually exclusive** by construction (see §2.1):

- **Stall** (failure) — the newest two files share the same initials and time since their last file exceeds **3 × the gradient length** observed between those two files.
- **Handoff** (happy case) — the previous user finished a run (the second- and third-newest files share initials) and the newest file's initials are not theirs (someone else took over, or an unattributable file arrived). The previous operator's queue is treated as ended *unconditionally*; no 3× check is performed.

Gradient length is estimated from the gap between two consecutive files of the same user. Real LC gradients on these instruments are never longer than ~2 hours; if the relevant pair's gap exceeds `MAX_GRADIENT_LENGTH_HOURS` (= 2 h), that pair is treated as belonging to different queues and the corresponding rule does not fire.

In both modes the DM goes only to the **prior** operator (the user whose queue ended). The new operator is never notified by this alert. Optionally a single "supervisor" Slack ID is CC'd on every alert.

Target users: instrument operators identified by their initials in raw file names (e.g. `MaSc`).

## 2. Detection logic (acceptance criteria)

The alert runs inside the existing `monitoring/` service, polled every `CHECK_INTERVAL_SECONDS = 60` seconds by `AlertManager`.

### 2.1 Per-instrument trigger conditions

For every instrument with at least two raw files in the DB:

1. Fetch the **three newest `RawFile` records** for that instrument (`file1`, `file2`, `file3`), ordered by `created_at` desc. The third may be absent if the instrument has only two files. No initials filter at this step.
2. Extract the initials token from each file name by matching the pattern `_<initials>_` (case-sensitive). A file's initials are "mapped" if the token is a key in `INSTRUMENT_USER_SLACK_IDS`. Files with no token or with an unmapped token are treated as having "no relevant initials".

Then evaluate the rules. They are mutually exclusive — by construction, **at most one rule can fire** for a given instrument in a given check (Rule A requires `file1.initials == file2.initials`; Rule B requires `file2.initials == file3.initials` *and* `file1.initials != file2.initials`).

**Rule A — Stall (active queue went quiet)**

- Preconditions: `file1` and `file2` share the *same* initials AND those initials are mapped.
- `gradient_length = file1.created_at - file2.created_at`.
- If `gradient_length > MAX_GRADIENT_LENGTH_HOURS` (= 2 h) → not a real gradient; treat `file1` as the start of a fresh queue; skip Rule A.
- If `gradient_length <= 0` (clock skew, simultaneous timestamps) → skip Rule A with a warning log.
- If `now - file1.created_at > QUEUE_END_THRESHOLD_MULTIPLIER × gradient_length` → emit a **stall** alert for that user. Alerted user = the user from `file1`/`file2` (shared mapped initials). Cooldown subject = `file1.id`.

**Rule B — Handoff (someone else took over)**

- Preconditions: `file3` exists AND `file2` and `file3` share the *same* initials AND those initials are mapped AND `file1`'s initials are not the same as `file2`/`file3` (covers all cases where `file1` has different mapped initials, an unmapped token, or no token at all).
- `prior_gradient_length = file2.created_at - file3.created_at`.
- If `prior_gradient_length > MAX_GRADIENT_LENGTH_HOURS` (= 2 h) → `file2` and `file3` are not part of the same queue; skip Rule B (the prior user didn't really have an active queue).
- Otherwise emit a **handoff** alert for the prior user (initials shared by `file2` and `file3`) unconditionally — no 3× check on the time since the handoff. Alerted user = `file2.user` = `file3.user`. Cooldown subject = `file2.id`. The new operator (`file1`) is never notified.

### 2.2 Edge cases

- Fewer than 2 raw files on the instrument → skip both rules.
- Only 2 raw files on the instrument (`file3` absent) → Rule B cannot fire (the prior user has no "previous two" to identify them). Rule A may still fire.
- Neither `file1` nor `file2` has mapped initials AND `file2`/`file3` don't share mapped initials → no alert.
- File name contains multiple `_<initials>_` matches → use the **first match** that exists in the mapping; if none exist, the file has "no relevant initials".
- Rule B with mapped-but-different `file1` initials (e.g. user X took over from user Y) → fires for Y (the prior user). X is not notified.
- Rule B with no-initials `file1` (e.g. an unattributable file from QC or another tool) → still fires for the prior user; `file1` just has no operator name.
- Single-file prior run (`file2.initials != file3.initials`, so the prior user had only one file) → Rule B does **not** fire. The prior user did not have a queue to end.

### 2.3 Cooldown / dedup

- Alert at most **once per `(instrument_id, subject_file_id)`**, where `subject_file_id` is the alerted user's most recent file:
  - For **stall**: `subject_file_id = file1.id` (the user's last file is the newest on the instrument).
  - For **handoff**: `subject_file_id = file2.id` (the prior user's last file, which is now second-newest).
- A single key per subject file means **the cooldown is shared between stall and handoff for the same user-last-file**: if a stall already fired about file F (while F was newest), and later a handoff would fire about file F (now F is second-newest), the cooldown suppresses the duplicate — the user already heard about it.
- This is the **only** time-based protection; no minimum-pause threshold and no per-user/per-instrument time window. No "kind" suffix on the key (rules are mutually exclusive per pair, so the key is unambiguous).
- The in-memory tracker pattern follows `InstrumentStallAlert._alerted_file_names`. Tracker entries are not pruned eagerly; they age out implicitly as files leave the top three on an instrument.

### 2.4 Message content

For each alert, send a Slack DM containing:

- A header line that depends on the kind:
  - **Stall:** instrument id, the inferred gradient length (minutes), the elapsed pause since the last file (minutes).
  - **Handoff:** instrument id, the new operator's initials (or `"unknown"` if `file1` has no mapped initials), timestamp of the takeover.
- The **up to three newest raw files on the instrument** (any initials, ordered newest → oldest). For each:
  - `original_name`
  - size (formatted as GB / MB)
  - precursor count (looked up via `augment_raw_files_with_metrics`, key `"precursors"` — may be `None` if no metrics yet; render as `n/a`).
  - If the instrument has only 2 files, render only those 2; do not pad. (Handoff cannot fire in that case anyway; stall can.)
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
  - `identifier = f"{instrument_id}:{subject_file_id}"` — `subject_file_id` is `file1.id` for stall, `file2.id` for handoff (see §2.3). No `kind` in the key: it's unambiguous because the two rules are mutually exclusive per file pair, and unifying the key is what gives stall/handoff a shared cooldown for the same user-last-file.
  - `payload` is a dataclass `QueueEndIssue(kind, instrument_id, alerted_initials, slack_user_id, new_operator_initials | None, gradient_length | None, pause | None, recent_files)`. `recent_files` is a list of **up to three** `(name, size_bytes, precursors)` tuples (newest first).
- `format_message` is not used in the standard way (no shared multi-issue message). Instead the alert exposes `dispatch(issues) -> None` which formats and sends a separate DM per issue/recipient.
- `QueueEndAlert.dispatch(issues)` does:
  - For each issue, build the message text (stall or handoff template) and the recipient list: `[slack_user_id] + ([SPECIAL_ALERT_SLACK_ID] if SPECIAL_ALERT_SLACK_ID else [])`, deduplicated.
  - For each recipient, wrap the `send_slack_dm(recipient, message, SLACK_BOT_TOKEN)` call in `try/except Exception`. On failure, log `(recipient, kind, instrument_id, exception)` at WARNING and **continue** to the next recipient / next issue. One bad Slack ID must not abort delivery for other recipients or break the monitor loop.

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
3. `test_no_alert_when_no_files_have_mapped_initials`.
4. `test_initials_pattern_matches_underscored_token_only` (e.g. `MaScfoo` should NOT match).
5. `test_rules_a_and_b_are_mutually_exclusive` (no single check ever fires both for the same instrument).

**Rule A — stall (`file1.initials == file2.initials`, gap ≤ 2 h, pause > 3× gradient)**
6. `test_stall_no_alert_when_pause_below_threshold` (pause = 2 × gradient_length).
7. `test_stall_alert_fires_when_pause_exceeds_threshold` (3.5 × gradient_length, gap = 30 min).
8. `test_stall_skips_when_gradient_length_exceeds_max` (gap = 3 h → no alert; treated as new queue start).
9. `test_stall_skips_when_gradient_length_zero_or_negative` (skip + log).
10. `test_stall_includes_recent_files_with_size_and_precursors` (3 files when available).
11. `test_stall_renders_two_files_when_instrument_has_only_two`.
12. `test_stall_cooldown_no_repeat_alert_for_same_subject_file_id`.
13. `test_stall_cooldown_releases_when_newer_file_appears`.

**Rule B — handoff (`file2.initials == file3.initials`, `file1.initials != file2.initials`, prior gap ≤ 2 h)**
14. `test_handoff_fires_when_prior_two_share_mapped_initials_and_newest_differs`.
15. `test_handoff_skips_when_only_two_files_on_instrument` (no `file3` → cannot identify prior queue).
16. `test_handoff_skips_when_prior_pair_does_not_share_initials` (single-file prior run; user did not have a queue).
17. `test_handoff_skips_when_prior_pair_initials_unmapped`.
18. `test_handoff_fires_when_newest_has_different_mapped_initials` (X took over from Y → alert Y).
19. `test_handoff_fires_when_newest_has_no_initials_token` (unattributable file → still alert prior user).
20. `test_handoff_fires_when_newest_has_unmapped_initials` (alert prior user only).
21. `test_handoff_skips_when_prior_gap_exceeds_max_gradient_length_hours` (prior pair > 2 h → not a real queue).
22. `test_handoff_cooldown_no_repeat_alert_for_same_subject_file_id`.

**Unified cooldown (stall + handoff share key)**
23. `test_unified_cooldown_handoff_suppressed_after_stall_for_same_user_last_file` — fire a stall about user-last-file F (while F is newest); later, with F now second-newest and a different-initials file newest, handoff for F must **not** re-fire (same `(instrument, F)` key already fired).

**Recipients & delivery**
24. `test_stall_recipient_is_shared_initials_user`.
25. `test_handoff_recipient_is_prior_file_user_not_new_operator`.
26. `test_special_id_cc_when_configured`.
27. `test_no_special_id_cc_when_not_configured`.
28. `test_recipients_deduplicated_when_user_id_equals_special_id`.
29. `test_dispatch_sends_separate_dm_per_recipient_per_issue` (asserts QueueEndAlert.dispatch fans out send_slack_dm; messages are not bundled across users).
30. `test_dispatch_continues_after_failed_send_slack_dm` — first recipient raises (e.g. `requests.HTTPError` or `ok: false`); second recipient still receives the DM; failure is logged with `(recipient, kind, instrument_id)`; the call to `_get_issues` for the next instrument is unaffected.
31. `test_alert_manager_dispatches_queueendalert_via_isinstance_branch` (lives in `test_alert_manager.py` if it exists, else in this file).

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
