# Monitoring module review

Date: 2026-10-23. Commit: 480b1093. Scope: `monitoring/` (12 alerts, manager, messenger clients, tests).
Concrete defects are tracked separately in `BOYSCOUT_2026-10-23.md`.

## 1. What drives complexity

### 1.1 Three overlapping identity schemes for "the same issue"

- 1.1.1 Cooldown key: `alert.name + identifier + kind` (`alert_manager.py:139`).
- 1.1.2 Once-only key: `Issue.unique_key`, tracked in a second set (`alert_manager.py:47`).
- 1.1.3 Suppression match: `class.__name__ + instrument_id + identifier + substring(details)` (`base_alert.py:71`).

`Issue` carries six fields, four of them identity-ish. The overlap is visible in
`PumpPressureAlert.get_cooldown_time_minutes`, which returns 0 "because the unique key already
prevents repeats". Every new alert author must pick among the three.

### 1.2 Alerts mix fetching and deciding

- 1.2.1 Seven of twelve alerts ignore `status_objects` (`del status_objects`) and query Mongo or
  HTTP themselves. The base interface fits only half the subclasses.
- 1.2.2 This inflates tests. `test_queue_stop_alert.py` patches `RawFile` 29 times and `datetime`
  24 times (1018 lines). `test_pump_pressure_alert.py` is 746 lines. The rule logic itself is pure
  and small.

### 1.3 Tuples and keyword explosions

- 1.3.1 `pump_pressure_alert.py`: 6-tuples indexed as `change[5]`, `pressure_changes[0][4]`.
  `_detect_pressure_increase` returns `(bool, list)`; the list has at most one element and the
  bool equals `bool(list)`.
- 1.3.2 `queue_stop_alert.py`: `_evaluate_rule_a` takes 10 keyword args unpacked from a list of
  3 files, then reassembled into `QueueStopIssue`.
- 1.3.3 `QueueStopIssue` is mutable only so `format_message` can mutate it with a DB query right
  before rendering (`refresh_recent_files`).

### 1.4 Dead or never-configured code

- 1.4.1 `InstrumentStallAlert` can never fire: `INSTRUMENT_STALL_INSTRUMENT_IDS` is an empty set
  nobody populates. Decision: keep the alert; the instrument set needs a YAML setting.
- 1.4.2 `SPECIAL_ALERT_SLACK_ID`, `PUMP_PRESSURE_GRADIENT_TOLERANCE`, `_send_direct_message_msteams`,
  `should_send_alert(cooldown_time_minutes=)`.
- 1.4.3 `CustomAlert` exists only to give `send_special_alert` a cooldown; it implements three
  abstract methods as no-ops.

### 1.5 Two names per alert

- 1.5.1 `Cases.X` via a 4-line `name` property on every subclass (48 lines of boilerplate); used
  for cooldowns and destinations.
- 1.5.2 `__class__.__name__`; used for suppressions and typed by admins in the webapp.
- Decision: the Python class name is the alert name everywhere. Drop `Cases` and the `name`
  property.

### 1.6 Config module with import-time side effects

- 1.6.1 `alerts/config.py` calls `sys.exit(1)` at import if the ops webhook is missing. Constants
  and runtime config loading share one module, so it cannot be imported elsewhere safely.

## 2. Simplifications, ordered by payoff

### S1. Collapse identity to two concepts

Keep `identifier` for cooldown and suppression context. Drop `unique_key` by letting once-only
alerts return a very long cooldown. Caveat: this touches the "retry on failed send" path in
`_handle_issues`; check it first. Minimum: remove the per-issue cooldown override in pump pressure.

### S2. Separate fetch from decide in the two big alerts

- Queue stop: one pure `detect(files: list[RecentFile], now) -> QueueStopIssue | None`, with
  `RecentFile` carrying `initials`. Rules A and B become ~15 lines each with one argument. Rule
  tests then need no `RawFile` or `datetime` patches.
- Pump pressure: a `PressureChange` dataclass; `_detect_pressure_increase` returns
  `PressureChange | None`; hoist `_is_within_tolerance` to module level and use the config constant.

### S3. Drop the mutate-then-render dance in queue stop

Do not capture `size` at detection. Query sizes in `format_message` from the stored file ids.
`QueueStopIssue` becomes frozen; `refresh_recent_files` disappears.

### S4. Delete dead code

Everything under 1.4.2. `InstrumentStallAlert` stays and gets its instrument set from YAML.

### S5. Use the class name as alert name

`BaseAlert.name` becomes `type(self).__name__`; drop `Cases` and every `name` property.
`ALERT_DESTINATIONS` is then keyed by class. Suppressions, cooldowns and the webapp admin share
one vocabulary.

### S6. Replace `CustomAlert` + `send_special_alert`

A small `AlertManager.send_once(key, message, cooldown_minutes)` using `last_alerts` directly.

### S7. Local cleanups

- `DiskSpaceAlert._get_threshold`: a 3-entry dict lookup written as 15 lines of if/else.
- `WebAppHealthAlert`: four exception branches producing four strings. One `RequestException`
  catch with `f"{type(e).__name__}: {e}"` halves the file and the 27 patches in its test.
- `_BYTES_PER_GB` duplicated between `queue_stop_alert.py` and `config.py`.

## 3. Quirks to decide on (not bugs)

- Q1. `should_send_alert` ORs across issues. If one instrument's disk alert is past cooldown,
  every issue in that batch is re-sent and has its timestamp reset. Intentional "full picture" or
  accident? Undocumented either way.
- Q2. After a restart, `is_first_check` marks all current issues as handled. Issues with
  `unique_key` are then never reported by that instance. A raw-file error occurring during a
  monitoring restart is silently lost. Resolved by persisted issue state
  (`SPEC_alert_issue_state.md`).
