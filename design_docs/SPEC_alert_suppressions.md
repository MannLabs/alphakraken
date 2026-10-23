# Spec: alert suppressions managed in the webapp

Status: to be re-implemented from scratch. Reference implementation on `refactor_alerts_IX`
(commit 84d82f64; `webapp/pages_/admin.py`, `webapp/pages_/impl/admin_utils.py`,
`shared/db/models.py:AlertSuppression`, `monitoring/alerts/base_alert.py:_apply_suppressions`).
Depends on the admin page from the `add_users` branch and on the decision that the alert name is
the Python class name (`design_docs/MONITORING_REVIEW_2026-10-23.md`, S5).

## Objective

Ops can mute specific alerts at runtime from the webapp, without editing `suppressions.yaml` and
redeploying. Rules can expire and carry a reason. Monitoring picks up changes with its next check.

## Decisions

- Document `AlertSuppression` in collection `alert_suppression`:
  `error_class`, `instrument_id`, `context`, `message` (all required, default `*`, max 64/64/256/256),
  `comment` (required, max 512), `expires_at` (optional, `None` = never), `created_by` (optional),
  `created_at_`.
- Wildcard `*` is `ALERT_SUPPRESSION_WILDCARD` in `shared/keys.py`.
- An issue is muted if all four fields match:
  - `error_class`: alert class name, exact, or `*`.
  - `instrument_id`: exact match on the instrument the issue relates to, or `*`. Issues of file
    systems, jobs, webapp and local disk have no instrument, so only `*` matches them.
  - `context`: exact match on the issue identifier, or `*`.
  - `message`: substring of `str(issue.details)`, or `*`.
- Active rules are those with `expires_at` `None` or in the future. Monitoring loads them once per
  check cycle and passes them to every alert. A muted issue still gets an `alert_issue` record
  with `muted = True` (`SPEC_alert_issue_state.md`) but no notification. When the rule expires
  and the issue is still detected, it is notified on the next cycle.
- `suppressions.yaml` is removed and no longer read.
- Admin page section "Alert suppressions": info text with the matching rules; table of all rules
  with an `expired` column, timestamps in display timezone; "Delete suppressions" expander with one
  delete button per rule; "Add suppression" form.
- Form: alert class (text, default `*`), instrument (selectbox: `*` plus known instrument ids),
  context (text, `*`), message (text, `*`), comment (text area), expires after (date, optional,
  not before today; stored as end of that day in display timezone), created by (text, optional).
  Writes respect `DISABLE_WRITE`.
- Validation (`validate_suppression`): alert class and comment non-empty; not all four match
  fields `*` (would mute everything). Fields are stripped before saving.
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

Monitoring: matching matrix (each field exact vs `*`, context exact not substring, message
substring, instrument `None` vs `*`, expired rule ignored by the interface query). Webapp:
`validate_suppression` cases, `get_suppressions_df` expired column, page tests for add and delete.

## Success criteria

- Add a rule in the webapp ⇒ matching alerts stop with the next monitoring check.
- Delete it or let it expire ⇒ alerts resume without restart.
- A rule with all four fields `*` is rejected.

## Open questions

- With class names as alert names, the alert class field could be a dropdown built from
  `BaseAlert.__subclasses__()`. Free text was chosen originally; revisit.
- Should `message` also be an exact match, for symmetry with `context`?
- Should rules with no instrument match non-instrument issues at all, or should those issues get
  a pseudo instrument id (e.g. the file system name)?
