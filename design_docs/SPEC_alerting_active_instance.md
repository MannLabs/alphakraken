# Spec: alerting active instance

Status: implemented (slice 5). Reference implementation on `refactor_alerts_IX`
(commits 79f81de6, 3318e4eb, 55b016e3; `monitoring/active_instance.py`). Revised 2026-10-23 for
persisted issue state (`SPEC_alert_issue_state.md`): the election now decides which instance
runs the DB-backed checks, not merely which one sends.

## Objective

Several identical monitoring instances run (compose profiles `infrastructure`, `workers`, `dbs`).
Exactly one, the active one, runs the DB-backed checks and notifies. The others only run the
DB-free tier. If the active one dies, another takes over within a few check intervals, warm,
because the issue state is in the DB. No coordination beyond MongoDB.

## Decisions

- Election by a single document in collection `alerting_active_instance`, `_id = "monitoring"`,
  fields `instance` (str) and `expires_at` (UTC datetime).
- Claim or renew in one atomic `find_one_and_update(..., upsert=True)` whose filter is
  `_id` and (`instance == self` or `expires_at < now`). If the filter does not match, the upsert
  tries to insert a second `_id` and raises `DuplicateKeyError`: this instance is not active.
- Timeout is 3 × `CHECK_INTERVAL_SECONDS` (180 s). Each loop iteration renews before checking.
- Instance id is `<KRAKEN_HOSTNAME>-<8 hex of uuid4>`, unique even for two instances on one host.
- Fail open: on any other `PyMongoError` (e.g. missing write permission) the instance logs the
  exception and acts as active. Duplicate alerts beat no alerts.
- Expiry is compared against the instance's own clock; host clocks must be roughly in sync.
- Non-active instances skip the DB-backed checks entirely. They renew the claim attempt and run
  the DB-free tier (`db_connection_error`, `db_timeout`, `startup`, `exception`, local disk,
  webapp health), which every instance sends with its own in-memory cooldown.
- Log a line on every active/inactive transition.
- DB access: user `MONGO_USER_ALERTING` / `MONGO_PASSWORD_ALERTING` with role `alertingRole`
  (read all, plus `find`/`insert`/`update` on `alerting_active_instance` and `alert_issue`).
  Monitoring connects as this user instead of `MONGO_USER_READ`. `misc/init-mongo.sh` creates
  role and user on fresh DBs; an upgrade guide covers existing deployments.
  `docker-compose.yaml` requires the two variables. The user is introduced with the issue state
  slice, since that slice is the first to write.

## Out of scope

- Splitting checks across instances.
- Handing over mid-cycle; a check that is running when the claim expires may still notify.

## Changes

- `monitoring/active_instance.py`: `ActiveInstanceElection(instance, timeout).try_become_active()
  -> bool`.
- `monitoring/main.py`: build instance id, renew claim each loop, run DB-backed checks only when
  active.
- `monitoring/alerts/config.py`: `ACTIVE_INSTANCE_TIMEOUT_SECONDS`.
- `misc/init-mongo.sh`, `docs/deployment.md`, upgrade guide: add the collection to the role.

## Testing strategy

Unit tests with a mocked collection: free claim, held by another and unexpired, held by self,
expired claim, `DuplicateKeyError`, `PyMongoError` ⇒ active. Main-loop test: not active ⇒
DB-backed checks not called, DB-free checks still called.

## Success criteria

- Three instances running, one alert condition ⇒ exactly one message.
- Kill the active instance ⇒ another notifies within 2 × timeout, without repeating issues
  already notified.
- Alerting DB user missing ⇒ all instances run the checks and send, with an error logged each cycle.

## Open questions

- None. The former questions (gating of `db_timeout`, DB load of non-active instances) are
  answered by the two-tier rule.
