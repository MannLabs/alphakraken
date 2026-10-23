# Spec: alert suppressions managed in the webapp

Status: implemented (slice 4). Reference implementation on `refactor_alerts_IX`
(commit 84d82f64; `webapp/pages_/admin.py`, `webapp/pages_/impl/admin_utils.py`,
`shared/db/models.py:AlertSuppression`, `monitoring/alerts/base_alert.py:_apply_suppressions`).
Depends on the admin page from the `add_users` branch and on the decision that the alert name is
the Python class name (`design_docs/MONITORING_REVIEW_2026-10-23.md`, S5).

## Objective

Ops can mute specific alerts at runtime from the webapp, without editing `suppressions.yaml` and
redeploying. Rules can expire and carry a reason. Monitoring picks up changes with its next check.

## Decisions

- Document `AlertSuppression` in collection `alert_suppression`:
  `alert`, `instrument_id`, `identifier`, `details` (all required, non-empty, default `*`, max
  64/64/256/256; named like the `AlertIssue` fields they match),
  `comment` (required, max 512), `expires_at` (optional, `None` = never), `created_by` (optional),
  `created_at_`.
- Wildcard `*` is `ALERT_SUPPRESSION_WILDCARD` in `shared/keys.py`.
- An issue is muted if all four fields match:
  - `alert`: alert class name, exact, or `*`.
  - `instrument_id`: exact match on the instrument the issue relates to, or `*`. Issues of file
    systems, jobs, webapp and local disk have no instrument, so only `*` matches them.
  - `identifier`: exact match on the issue identifier, or `*`.
  - `details`: substring of the issue as rendered in the message (`format_message([issue])`),
    or `*`.
- Active rules are those with `expires_at` `None` or in the future. Monitoring loads them once per
  loop, before the DB-free checks, and keeps the last read ones while the DB is down. A muted
  issue is skipped: no `alert_issue` record, no notification. When the rule expires and the issue
  is still detected, it is treated like any other detected issue.
- `suppressions.yaml` is removed and no longer read.
- Admin page section "Alert suppressions": info text with the matching rules; table of all rules
  with an `expired` column, timestamps in display timezone; "Delete suppressions" expander with one
  delete button per rule; "Add suppression" form.
- Form: alert class (text, default `*`), instrument (selectbox: `*` plus known instrument ids),
  identifier (text, `*`), details (text, `*`), comment (text area), expires after (date, optional,
  not before today; stored as end of that day in display timezone), created by (text, optional).
  Writes respect `DISABLE_WRITE`.
- Validation (`validate_suppression`): all match fields and the comment non-empty; not all four
  match fields `*` (would mute everything). Fields are stripped before saving.
- DB access: `webappRole` gets `find`/`insert`/`update`/`remove` on `alert_suppression`.
  Monitoring needs read only. Upgrade guide: note yaml rules, grant privileges, re-create rules.
- Interface (`shared/db/interface.py`): `get_alert_suppressions()` ordered by `created_at_`
  descending, `get_active_alert_suppressions()`, `add_alert_suppression(...)`,
  `delete_alert_suppression(id)`.

## Out of scope

- Editing an existing rule (delete and re-add).
- Showing how many issues a rule currently mutes.
- Access control of the admin page (handled by `add_users`).

## Changes

- `shared/keys.py`, `shared/db/models.py`, `shared/db/interface.py`.
- `monitoring/alerts/base_alert.py`: `get_issues(status_objects, suppressions)`, matching as above.
- `monitoring/alert_manager.py`: load active suppressions once per cycle.
- `webapp/pages_/admin.py`, `webapp/pages_/impl/admin_utils.py`.
- `misc/init-mongo.sh`, `docs/upgrade_guides/upgrade_alert_suppressions.md`.
- Delete `monitoring/suppressions.yaml` and its loader.

## Testing strategy

Monitoring: matching matrix (each field exact vs `*`, identifier exact not substring, details
substring, instrument `None` vs `*`, expired rule ignored by the interface query). Webapp:
`validate_suppression` cases, `get_suppressions_df` expired column, page tests for add and delete.

## Success criteria

- Add a rule in the webapp ⇒ matching alerts stop with the next monitoring check.
- Delete it or let it expire ⇒ alerts resume without restart.
- A rule with all four fields `*` is rejected.

## Open questions

- With class names as alert names, the alert class field could be a dropdown built from
  `BaseAlert.__subclasses__()`. Free text was chosen originally; revisit.
- Should `details` also be an exact match, for symmetry with `identifier`?
- Should rules with no instrument match non-instrument issues at all, or should those issues get
  a pseudo instrument id (e.g. the file system name)?
