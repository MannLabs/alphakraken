# Review of DB_INTERFACE_DESIGN.md

Reviewed 2026-11-03 against commit 87937c8c. The current-state analysis (sections 1 to 3)
checks out. The problems are in the target design and the migration plan.

## 1. Claims that are wrong at the code

1.1. **"Airflow call sites unchanged except two exception imports" (5.2) is false.** Airflow
puts `str(settings.id)` (an ObjectId) into XCom at `processor_impl.py:101` and reads it back
with `get_settings_by_id`. Step 5.5 (settings id becomes `unique_name`) therefore touches
Airflow, and DAG runs in flight across that deploy carry ObjectIds in XCom. Say so and decide
how to handle it.

1.2. **5.2 is not shippable as described.** It makes `get_project_settings` return
dataclasses, but webapp pages and `shared/settings_scope_resolver.py` read `ps.settings.name`
and `str(ps.id)` on those objects. The embedded `settings` only arrives in 5.5. Either 5.2
already embeds `Settings` into `ProjectSettings`, or 5.2 ports those call sites. Pick one.

1.3. **"The one join implementation" (2.1, 5.3) hides a behaviour change.** The three
implementations disagree on what "latest" means. `augment_raw_files_with_metrics` returns the
oldest metrics per type after a re-quant, the MCP copy returns the newest, the webapp returns
natural order. See `BOYSCOUT_2026-11-03.md`, item 3. Step 5.3 should state which semantics win
and that REST and the pump pressure alert change behaviour.

1.4. **The lease API loses the fail-open rule.** `try_acquire(...) -> bool` cannot express what
`active_instance.py` does today: duplicate key means "not active", any other Mongo error means
"act as active". Either `try_acquire` raises `DatabaseUnavailable` and the caller fails open,
or the fail-open moves into the lease. Say which.

1.5. **CI has no MongoDB.** Section 5 says `_mongo/` tests run against a real MongoDB service
container, but `branch-checks.yaml` has none. That infra step is missing from the plan.
`shared/tests/db/test_interface.py` has 1754 lines of MagicMock querysets that cannot be
ported, only rewritten. The doc does not mention this cost.

## 2. Design gaps

2.1. **The doc never says why Mongo should be replaceable.** That goal drives most of the cost:
mappers, a second type hierarchy, the ruff ban, tz handling, rewritten DB tests. The measurable
value (sections 2 and 3.1 to 3.6) comes from deduplication and hiding the ORM, which a lighter
design also gives: one query module per aggregate inside `shared/db`, ban `.objects` outside
it, keep mongoengine documents as return types. If there is a concrete plan to leave Mongo,
state it. If not, section 6 and the Postgres notes are speculation and the dataclass layer
needs its own justification.

2.2. **Metrics `fields` projection vs harmonization in the mapper (4.2).** If the mapper
renames legacy keys, `fields=["evosep_pump_hp_pressure_max"]` must also project
`msqc_evosep_pump_hp_pressure_max`, or the mapper has nothing to rename. The legacy map must
live in `_mongo` and apply in both directions. Today `pump_pressure_alert.py:69-72` lists both
names by hand.

2.3. **`update(raw_file_id, **fields)` (4.2) contradicts 4.8.** 4.8 requires keyword-only,
named parameters on write functions. Today's `update_raw_file` has a typed signature with a
`_NO_UPDATE` sentinel. Keep that; drop `**fields`.

2.4. **Datetimes (4.5) only fix the read side.** Writes use `default=datetime.now`, naive local
time. The mapper assuming UTC is only right because every writer runs in a UTC container. The
write side should become `datetime.now(UTC)` in the same step; otherwise 7.5 is half a
decision.

2.5. **`queue_stop` maps to `latest_per_instrument`**, but today it derives instruments from
`RawFile.distinct("instrument_id")`, not from `KrakenStatus`. `instrument_ids()` in
`status.py` gives a different set (registered vs has files). Say which, or the alert changes
behaviour silently.

2.6. **`asdict()` for DataFrames (4.6).** Dataclasses were chosen over pydantic because of the
cost of validating thousands of rows, yet `asdict` deep-copies recursively and is slow for
exactly that case. Use `vars(x)` or a flat `to_row()`.

2.7. **Test patching convention is unstated.** "Client tests patch `shared.db.*`" only works if
clients import modules (`from shared.db import raw_files`), not functions. Today every client
does `from shared.db.interface import get_x`. State the import convention, or patch targets
differ per file.

2.8. **Ruff exemptions (4.7)** need the test directories for `_mongo` too, or the real-Mongo
tests cannot import pymongo.

## 3. Minor

3.1. 2.7 says 63 test patches of `connect_db`; 196 mentions across 9 test files. Immaterial.

3.2. `Settings` has two unique keys, `number` and `(name, version)`. Keeping `number` is fine,
but 6.3 should note that `number` currently serves display only.

3.3. Section 7 lists five decisions to confirm, but 2.1 above decides whether the others
matter. Add it as 7.0.

## Summary

Current-state analysis is accurate. Five concrete errors (1.1 to 1.5); the largest are that
step 5.2 is not shippable as written and that Airflow is not untouched. Main weakness: the
most expensive property, provider independence, is not motivated anywhere. One real bug found
and logged in `BOYSCOUT_2026-11-03.md`, item 3.
