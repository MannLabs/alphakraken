# Monitoring rebuild: behavior changes to carry over

Date: 2026-10-23. Source: commits `add_users..refactor_alerts_IX` (480b1093), excluding the two
features specified in `SPEC_alerting_active_instance.md` and `SPEC_alert_suppressions.md`.

Each row is a behavior that exists on the stack but not on `main`. Tick it when the rebuild has it
(with a test), or mark it deferred. "Origin" is the stack commit to diff against for details.

Status: `open` | `done` | `deferred` | `dropped`

| # | Behavior | Origin | Verify with | Status |
|---|---|---|---|---|
| B1 | Destinations configurable per alert class: `ops`, `business`, `user` (DM), any combination. Default `ops`. Pump pressure → `business`, queue stop → `user`. | f5486bfb | test: destination table drives `_dispatch` | open |
| B2 | A failing send to one destination or one DM recipient is logged and does not abort the others. | f5486bfb | test: first webhook raises, second still called | open |
| B3 | Business webhook URL falls back to the ops URL; if both are equal the message is sent once. | f5486bfb | test: identical URLs → one post | open |
| B4 | DM recipient is derived from `Issue.relevant_file` for every alert that has one: queue stop (subject file), raw file error, S3 upload failure, instrument stall (last file), pump pressure (younger file of the change, or the high-pressure file). | f5486bfb, 612ebe8c | test per alert: issue carries the expected file | open |
| B5 | A `user`-destination issue with no resolvable recipient is logged, skipped, and still counts as handled. | f5486bfb | test: no recipients → no send, cooldown set | open |
| B6 | Suppression `error_class` accepts `*`; suppressions carry `instrument_id` (exact or `*`); every issue carries the instrument it relates to, `None` for file systems, jobs, webapp, local disk. | c97454d2 | test: `*`/instrument rule mutes all alerts of that instrument only | open |
| B7 | Suppression `context` matches the issue identifier exactly, not as substring. | 6d361c5f | test: `inst1` does not match `inst10` | open |
| B8 | Pump pressure absolute-threshold issues use the plain instrument id as identifier plus a `kind`; cooldown is keyed by identifier and kind. Absolute → default cooldown, increase → none. | 6c80ea0b | test: cooldown keys differ per kind; suppression context `inst1` hits both kinds | open |
| B9 | The first occurrence of an issue is sent immediately, whatever its cooldown. (Before: cooldowns above 121 min, i.e. `file_remover` health check at 720 min, delayed the first alert by ~10 h.) | 492a26fd | test: 720 min cooldown, first issue sent at once | open |
| B10 | Issues are marked reported and their cooldown set only after a successful send. Blocked or failed sends are retried next check. (Before: stateful alerts marked issues in `_get_issues`, losing them.) | 2288dc9f | test: send raises → issue raised again next check | open |
| B11 | After a DB connect failure the check cycle is skipped, so only the `db_connection_error` alert is sent, not a second `exception` alert. | ba9d023f | test: `connect_db` raises → `check_for_issues` not called | open |
| B12 | `StaleStatusAlert` accepts tz-aware `updated_at_` (was `pytz.utc.localize`, which raises on aware datetimes). | ba9d023f | test: aware datetime input | open |
| B13 | Once-only reporting via `unique_key`, kept in the manager: stall `(instrument, last file)`; pump increase `(instrument, changes)`; pump absolute `(instrument, kind, file)`; queue stop `(instrument, subject file)` shared by stop and handoff; raw file error `(file, status_details)`; S3 `(file,)`. | 982ff34c | test per alert: same key twice → one report | open |
| B14 | Raw file error and S3 upload failure alerts are stateless: files in `ERROR` / `UPLOAD_FAILED` updated within the last 24 h, deduplicated by B13. (Before: 5 min window diffed against in-memory previous statuses.) A file failing again with different `status_details` is reported again. | 92f2ee9b | test: lookback filter, re-report on changed details | open |
| B15 | Known initials for queue-stop detection are the initials of active users in the DB; DM recipients are users with a `messenger_id`. The hard-coded `INSTRUMENT_USER_SLACK_IDS` is gone. | 480b1093 | test: unknown token ignored; user without messenger id → no recipient | open |
| B16 | Default `format_message`: `title` line plus one `- \`identifier\`: details` line per issue; alerts without a custom format set `title`. | 81566149 | test: default rendering | open |

## Known side effects worth a decision

- B5 and B15 together: a queue stop for a user without `messenger_id` is detected, logged as
  "no recipients", and marked handled. It is never delivered.
- B10: if one of several DM recipients fails, all issues of that alert are retried, so the
  recipients that succeeded get the message again.
- B13 and the first-check suppression: resolved by persisted issue state
  (`SPEC_alert_issue_state.md`); the first-check rule is not rebuilt.
- `should_send_alert` ORs across issues: one issue past cooldown re-sends the whole batch and
  resets every issue's timestamp. Decided (plan D2): not rebuilt, only due issues are sent.
