# Implementation Plan: Queue-Stop User Alert

Companion to [`SPEC.md`](./SPEC.md). Resolves four ambiguities and one architecture concern surfaced during planning.

## Decisions taken (overrides / clarifications vs SPEC)

| # | Question | Decision |
|---|---|---|
| 1.1 | How to look up precursor count | **Deferred** — no precursors in the message body for now. SPEC §2.4, §4, §5 references to precursors are skipped. |
| 1.2 | `format_message` for `QueueEndAlert` | Return empty string (no-op). |
| 1.3 | First-check (post-restart) DMs | Suppress, like other alerts. Tracker still populated so files don't re-fire later. |
| 1.5 | CC body to `SPECIAL_ALERT_SLACK_ID` | Identical to primary recipient's body. |
| Arch | Slack-specific `send_slack_dm` vs centralized routing | **Centralized.** Public `send_dm` in `messenger_clients.py` dispatches internally to `_send_slack_dm` (today) or `_send_msteams_dm` (stub raises `NotImplementedError`). Alerts call `send_dm` only; never platform-specific functions. |

## Phase 0 — Pre-work (read-only)

- **0.1** Confirm `RawFile.size` is populated for files that reach the top-3 (spot-check production data shape before relying on it for message rendering).
- **0.2** Confirm production env yaml secret-substitution pattern for `slack_bot_token`. If production yaml is committed plaintext, the bot token must come via env var, not yaml — flag and ask before checking in any placeholder.

## Phase 1 — Foundation (config + transport, no behavior)

Independent; can land before the alert exists.

### 1.1 `shared/yamlsettings.py`
- Add `YamlKeys.SLACK_BOT_TOKEN = "slack_bot_token"`.
- **Acceptance:** imports clean; key referenced only from `monitoring/alerts/config.py`.

### 1.2 `envs/alphakraken.{local,sandbox,production}.yaml`
- Add `general.notifications.slack_bot_token: ""` in local; env-substituted placeholder in sandbox/production after 0.2.
- **Acceptance:** monitor service still boots in local with empty token.

### 1.3 `monitoring/messenger_clients.py` — centralized DM routing
- **Public:** `send_dm(message, recipient_id, *, message_type=AlertTypes.ALERT) -> None`. Reads credentials from `alerts.config`; dispatches by what is configured:
  - `SLACK_BOT_TOKEN` set → `_send_slack_dm(...)`.
  - Future MS Teams DM creds set → `_send_msteams_dm(...)`.
  - Nothing configured → log warning + return (mirrors `send_message`'s no-crash policy).
- **Private:** `_send_slack_dm(message, user_id, token, message_type)` — POST `https://slack.com/api/chat.postMessage` with Bearer token, JSON `{channel, text}`, env prefix + emoji matching `_send_slack_message`. `raise_for_status()`; raise on `ok: false`.
- **Private stub:** `_send_msteams_dm(...)` — `raise NotImplementedError("MS Teams DMs not yet supported")`.
- **No public `send_slack_dm`** — alerts must call `send_dm`.
- **Acceptance** (tests in new `monitoring/tests/test_messenger_clients.py`):
  - `send_dm` with Slack token configured → calls `_send_slack_dm` with correct args.
  - `send_dm` with no credentials → logs warning, returns, does not call `requests.post`.
  - `_send_slack_dm` direct: endpoint URL, Bearer header, payload shape, non-prod `[<env>]` prefix, `ok: false` raises, HTTP non-2xx raises.
  - `_send_msteams_dm` raises `NotImplementedError`.

## Phase 2 — Alert core

### 2.1 `monitoring/alerts/config.py`
- Add constants: `QUEUE_END_THRESHOLD_MULTIPLIER = 3`, `MAX_GRADIENT_LENGTH_HOURS = 2`, `INSTRUMENT_USER_SLACK_IDS: dict[str, str] = {}`, `SPECIAL_ALERT_SLACK_ID: str | None = None`.
- Load `SLACK_BOT_TOKEN` via `get_notification_setting(YamlKeys.SLACK_BOT_TOKEN)`; fall back to `""` with WARNING on `KeyError`.
- Add `Cases.QUEUE_END = "queue_stop"`.

### 2.2 `monitoring/alerts/queue_stop_alert.py` — `QueueEndAlert`
- Module-level `INITIALS_PATTERN = re.compile(r"_([A-Za-z]{2,8})_")`.
- `@dataclass QueueEndIssue(kind, instrument_id, alerted_initials, slack_user_id, new_operator_initials, gradient_length, pause, recent_files)` — `recent_files` is `list[tuple[name, size_bytes]]` (precursors deferred).
- `_alerted_subject_files: set[tuple[str, str]]` for `(instrument_id, subject_file_id)` dedup. Tracker follows `InstrumentStallAlert._alerted_file_names` pattern; not pruned eagerly.
- `_get_issues`:
  - Iterate instruments via `RawFile.objects.distinct("instrument_id")`.
  - For each, fetch top-3 `RawFile` (`id`, `original_name`, `created_at`, `size`, `instrument_id`) ordered by `created_at` desc.
  - Extract initials via `INITIALS_PATTERN` (first regex match that is a key in `INSTRUMENT_USER_SLACK_IDS`; non-matching token → "no relevant initials").
  - Evaluate Rule A (stall) then Rule B (handoff) — mutually exclusive by construction (Rule A requires `file1.initials == file2.initials`; Rule B requires `file2.initials == file3.initials` AND `file1.initials != file2.initials`).
  - Apply per-`(instrument, subject_file_id)` dedup before returning.
  - Return `[(identifier, QueueEndIssue), …]` where `identifier = f"{instrument_id}:{subject_file_id}"`.
- `format_message` → `return ""` (no-op; see decision 1.2).
- `name` → `Cases.QUEUE_END`.
- **`dispatch(issues)`**:
  - For each issue, render stall/handoff template (header + up to three `name (size)` lines).
  - Recipient list: `[slack_user_id, SPECIAL_ALERT_SLACK_ID]` deduped (drop `None` and drop dupes if special ID equals alerted user's ID).
  - For each recipient: `try: send_dm(message, recipient_id) except Exception:` → WARNING log with `(recipient, kind, instrument_id, exception)`, continue to next recipient / next issue.
- **Alert imports `send_dm`, never `_send_slack_dm`** — platform-agnostic.

### 2.3 `monitoring/alerts/__init__.py`
- Export `QueueEndAlert` (add to `__all__` + import line).

### 2.4 `monitoring/alert_manager.py`
- Add `QueueEndAlert()` to `self.alerts`.
- In `_handle_issues`, refactor delivery branch:
  ```python
  if isinstance(alert, QueueEndAlert):
      if not suppress_alerts:
          alert.dispatch(issues)
      else:
          logging.info(f"Suppressed QueueEndAlert dispatch ({len(issues)} issues)")
  else:
      message = alert.format_message(issues)
      if not suppress_alerts:
          send_message(message, alert.get_webhook_url())
      else:
          logging.info(f"Suppressed alert for {alert_name}: {message}")
  ```
- `set_last_alert_time` loop stays outside the branch — first-check identifiers get recorded so the per-id cooldown prevents post-restart spam.

## Phase 3 — Tests

### 3.1 `monitoring/tests/alerts/test_queue_stop_alert.py`
- Implement SPEC §5 tests #1–#30, with these adjustments:
  - **#10** renamed to `test_stall_includes_recent_files_with_size` — drop precursor field assertions.
  - Drop `augment_raw_files_with_metrics` from the mocks list.
  - **Patch target for delivery assertions:** `monitoring.alerts.queue_stop_alert.send_dm` (not `send_slack_dm`).
- Mocks: patch `RawFile`, `INSTRUMENT_USER_SLACK_IDS`, `SPECIAL_ALERT_SLACK_ID`, `MAX_GRADIENT_LENGTH_HOURS`, `QUEUE_END_THRESHOLD_MULTIPLIER`, `send_dm`. Freeze `datetime.now(pytz.UTC)`.

### 3.2 `monitoring/tests/test_alert_manager.py`
- Add test #31 (`test_alert_manager_dispatches_queuestopalert_via_isinstance_branch`): mock `QueueEndAlert.dispatch`, assert it is called and `send_message` is not.

### 3.3 `monitoring/tests/test_messenger_clients.py` (new file)
- Covered by 1.3 acceptance criteria above.

## Phase 4 — Verification

```bash
python -m pytest monitoring/tests/alerts/test_queue_stop_alert.py \
                 monitoring/tests/test_alert_manager.py \
                 monitoring/tests/test_messenger_clients.py
pre-commit run --all-files
```

Manual smoke (optional, requires real bot token): put your own Slack ID in `INSTRUMENT_USER_SLACK_IDS`, run `python monitoring/main.py` against a local DB seeded with two same-initials files >3× a small gradient apart → confirm DM arrives.

## Out of scope (SPEC §6 "Ask first" — not taken on here)

- Generalizing the `isinstance` branch into a `BaseAlert.send` hook.
- Splitting `QueueEndAlert` into separate stall and handoff classes.
- Moving `INSTRUMENT_USER_SLACK_IDS` into YAML or Mongo.
- Changing `MAX_GRADIENT_LENGTH_HOURS` or `QUEUE_END_THRESHOLD_MULTIPLIER`.
- Unifying channel send + DM send into a single function (two functions, parallel structure, is the right level — different inputs and credentials).

## Risks

- **R1 — `RawFile.objects.distinct("instrument_id")`** could be slow on large datasets. Mongoengine `.distinct()` is typically indexed; verify in sandbox before production.
- **R2 — Tracker memory growth:** `_alerted_subject_files` is never pruned. Bounded by instrument count × monitor process lifetime; flag if restarts become rare.
- **R3 — SPEC drift:** SPEC.md still references precursors. After implementation lands, recommend marking precursors as deferred in SPEC.md. Not touched as part of this implementation unless asked.
- **R4 — Slack `chat.postMessage` to a user ID** requires `im:write` scope and the user to have opened the bot's DM at least once, or the bot to share a channel with the user. Operational, not code — confirm with workspace admin before rollout.
- **R5 — Credential-implicit platform dispatch:** `send_dm` picks the platform from whatever credentials are present. If both Slack and (future) MS Teams DM creds are configured simultaneously, Slack wins by `if/elif` order. Fine today; should grow a configurable preference when the second platform actually lands. Note this in a code comment.

## Commit sequence

1. `feat(messenger): add centralized send_dm with Slack impl + MS Teams stub` (Phase 1.1 + 1.2 + 1.3).
2. `feat(monitoring): add QueueEndAlert with stall + handoff rules` (Phase 2).
3. `test(monitoring): cover QueueEndAlert, send_dm, AlertManager isinstance branch` (Phase 3).
