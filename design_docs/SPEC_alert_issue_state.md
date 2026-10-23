# Spec: persisted alert issue state

Status: implemented (slice 2, muted flag with slice 4). Replaces the in-memory cooldown dict, the
once-only key set, the per-alert memory sets and the "first check suppresses everything" rule.

## Objective

Every issue the monitoring detects has a record in MongoDB with when it was last notified.
Cooldown and once-only reporting are properties of that record. Restarts lose nothing and a
takeover by another instance is warm. The record is notification memory, not a lifecycle: the
monitor does not track whether an issue is still present (dropped on 2026-10-23, together with
resolved notifications, to keep the design small).

## Decisions

- Collection `alert_issue`, one document per `(alert, key)`:
  `alert` (class name), `key` (str, see below), `identifier`, `instrument_id`, `relevant_file`,
  `details` (str rendering), `first_seen`, `last_notified` (`None` = never).
- `key` is an explicit, stable string built by the alert, never a tuple of floats or a repr.
  Default is `identifier`. Details-dependent keys use a short stable digest of the details.
  Examples: stall `"<instrument>:<last file>"`, raw file error `"<file>:<digest(details)>"`,
  pump increase `"<instrument>:<younger file>:<older file>"`, pump high `"<instrument>:<file>"`.
- Two tiers of alerts:
  - DB-backed (everything that reads Mongo): governed by `alert_issue` records; run and
    notified by the active instance only (see `SPEC_alerting_active_instance.md`).
  - DB-free (`db_connection_error`, `startup`, `exception`, local disk, webapp health):
    in-memory cooldown via `AlertManager.send_once`, sent by every instance. These must work
    while the DB is down; they apply the last suppressions that could be read. (`db_timeout` is
    folded into `db_connection_error`: a server selection timeout is a connection failure.)
- One cycle on the active instance:
  1. Load all records once.
  2. Detect issues (`detect(snapshot, now)` per alert); muted issues are skipped.
  3. Detected, no record ⇒ insert and notify.
  4. Detected, record exists ⇒ notify if `last_notified` is `None` or
     `now > last_notified + cooldown`; `cooldown = None` ⇒ never again.
  5. Not detected ⇒ nothing. An issue that disappears and comes back keeps its cooldown.
- `last_notified` is written after a successful send. A failed send leaves it unchanged, so the
  issue is retried next cycle. Races between instances are prevented by the election, not by the
  record; the fail-open case accepts duplicates.
- No first-check special case. On the first deploy, every currently open issue is notified once.
  The upgrade guide says so.
- Retention: a TTL index on `first_seen` (90 days) bounds the collection; a once-only issue
  recurring after that is notified again. The alerting user may not create indexes, so
  `misc/init-mongo.sh` and the upgrade guide create it.
- DB access: `alertingRole` gets `find`/`insert`/`update` on `alert_issue`. The webapp reads it
  later for the ops dashboard (`webappRole` already has read).

## Out of scope

- Resolved notifications and an issue dashboard: both would need the monitor to track
  whether an issue is still present, which this design deliberately does not.
- Grouping several issues of one alert into one message across cycles: a cycle's due issues of
  one alert class still go out as one channel message, as today.
- Editing records from the webapp.

## Changes

- `shared/db/models.py`: `AlertIssue`. `shared/db/interface.py`: `get_open_alert_issues()`,
  `upsert_alert_issue(...)`, `mark_alert_issues_resolved(ids, now)`,
  `set_alert_issue_notified(id, now)`.
- `monitoring/alerts/base_alert.py`: `Issue.key`, `BaseAlert.cooldown_minutes: int | None`,
  `get_cooldown_minutes(issue)` for the `file_remover` case.
- `monitoring/alert_manager.py`: the cycle above; `send_once` for the DB-free tier.
- `misc/init-mongo.sh`, `docs/upgrade_guides/upgrade_alert_refactoring.md`.

## Testing strategy

Manager tests as a table over (record state, cooldown, suppression, detected?) ⇒ (write, notify).
Failed send ⇒ `last_notified` unchanged and retried. Per alert: key is the same for the same
issue across two cycles and differs for a different one. DB-free alerts send with the DB mocked
as unreachable.

## Success criteria

- Restart the monitoring during an open issue ⇒ no repeated message, no lost later message.
- An issue that flaps around its threshold ⇒ one message per cooldown, not per cycle.
- DB unreachable ⇒ `db_connection_error` is still sent.

## Open questions

- None. Decided 2026-10-23: channel messages do not show how long an issue has been open;
  records expire 90 days after first seen via TTL index.
