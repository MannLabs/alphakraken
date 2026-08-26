# Bycatch

## 2026-08-26 — requirements pins contradict the constraints file they declare

Found while building an Airflow 3.3.1 scratch env to verify the Airflow 3 prep changes
(`design_docs/AIRFLOW3_A_PREP_CHANGES.md` §4.1).

Commit: `d5016e7755081013b3f37b8c7dc1976b16b51bdb`

Several direct pins are *newer* than the version pinned by the constraints file passed on the
same line, and `shared/requirements_shared.txt` pins a package the constraints file also pins:

| Requirement | Declared pin | `constraints-2.11.0/constraints-3.11.txt` says |
|---|---|---|
| `airflow_src/requirements_airflow.txt:3` `apache-airflow-providers-ssh` | `4.1.6` | `4.1.0` |
| `airflow_src/requirements_airflow.txt:7` `apache-airflow-providers-amazon` | `9.17.0` | `9.8.0` |
| `shared/requirements_shared.txt:2` `pymongo` | `4.7.2` | `4.10.1` |

`airflow_src/Dockerfile:24-27` installs `requirements_airflow.txt` and
`requirements_shared.txt` in one `pip install`, so the `--constraint` lines apply to the whole
resolution, including `pymongo`.

A strict resolver rejects this outright — `uv pip install` with the constraints file fails with
`Because you require pymongo==4.7.2 and pymongo==4.10.1, we can conclude that your requirements
are unsatisfiable`. Whether **pip** also fails could not be checked here (pip cannot reach PyPI
through this sandbox's TLS proxy), so it is unclear whether the image build currently succeeds by
luck, by pip leniency, or has simply not been rebuilt since the pins diverged.

Not caused by, and not affecting, the Airflow 3 prep changes — the mismatch predates them.

**Why it matters:** `AIRFLOW3_B_MIGRATION_GUIDE.md` §2 repoints every constraint URL to
`constraints-3.3.1` at once, where the same three pins diverge again (`pymongo` 4.17.0,
`ssh` 6.0.1, `amazon` 9.35.0). If pip has been tolerating the violation, that step turns a silent
inconsistency into a hard build failure at the worst moment.

**Suggested check:** run `docker build -f airflow_src/Dockerfile .` on the current tree. If it
succeeds, decide deliberately whether to keep overriding constraints (and drop the misleading
`--constraint` flags) or to align the pins with the constraints file.

## 2026-08-26 — `on_failure_callback` XCom fallback is dead code

Found while auditing `get_xcom()` call sites for Airflow 3 `task_ids` semantics.

Commit: `d5016e7755081013b3f37b8c7dc1976b16b51bdb`

`airflow_src/plugins/callbacks.py:29-38` falls back to reading the raw file id from XCom when it is
not in the DAG params:

```python
raw_file_id = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]
except KeyError:
    raw_file_id = get_xcom(ti, key=XComKeys.RAW_FILE_ID)   # <- never succeeds
```

`XComKeys.RAW_FILE_ID` is **never pushed** anywhere in the codebase — `grep -rn "XComKeys.RAW_FILE_ID"`
returns only this read. So the fallback always raises `KeyError`, is swallowed, and logs
"could not find raw file id in dag params nor xcom. Not updating status in db."

Effect: for DAGs without a `raw_file_id` param (`instrument_watcher`, `file_remover`), a task
failure never updates the raw file status, contrary to the docstring "Assumes that 'raw_file_id' is
in the XCom."

Either push `XComKeys.RAW_FILE_ID` where it is known, or drop the fallback and the misleading
docstring. Independent of the Airflow 3 migration.
