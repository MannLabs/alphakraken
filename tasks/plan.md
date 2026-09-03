# Implementation Plan: Named runners (D3-lite)

Spec: `SPEC.md` at `d24994dd`. Task list: `tasks/todo.md`.

## Overview

Replace the single `CLUSTER_VIEW` / `Settings.job_engine` pair by a list of named runners, each
with engine, os, view and SSH prefix. Dissolve the yaml `locations` block into `runners[].view`,
`mounts` and `backup.backup_base_path`. Pure refactoring except validation moving to the relative
parts (spec 1.2.2).

## Strategy: old and new coexist until the last phase

The spec's end state has no fallback, but the *path there* does not need to be one commit.
Every task below leaves the repo deployable with the in-repo yamls:

1. Runners are added next to `locations` (no consumer yet).
2. Job dispatch switches to runner names (DB rename).
3. Path resolution and SSH switch to the runner.
4. Only then is `locations` / `CLUSTER_VIEW` deleted, when nothing reads it any more.

Intermediate yamls carry both `locations` and `runners`. That is duplication for a few commits,
not backwards compatibility: nothing tolerates a *missing* key at any point.

## Dependency graph

```
T1 shared/runners.py + _test_ stub ── T2 yamls gain runners: + consistency test
        │
        ├── T3 QuantingEnv.runner, factory dispatches on Runner
        │        └── T4 Settings.runner replaces job_engine (DB, webapp)
        │                 └── T5 migration script
        │
        ├── T6 _check_content explicit list         (needs QuantingEnv.runner from T3)
        │        └── T7 prepare_job via runner.view   (windows strings must pass T6)
        │
        └── T8 slurm handler + SSH prefix from runner (needs factory from T3)

T9  backup.backup_base_path replaces CLUSTER_VIEW in get_backup_base_path
T10 DOCKER_HOST_VIEW from MOUNTS_PATH env
T11 mounts: block, mount.sh, drop locations from yamls   (needs T7, T8, T9: no CLUSTER_VIEW reader left)
T12 delete CLUSTER_VIEW, YamlKeys.LOCATIONS, yaml_locations helper   (needs T11)
T13 docs sweep, 9.1/9.2 greps
```

## Architecture decisions

- **Two commits for the DB rename (T3, T4).** T3 introduces `QuantingEnv.runner` and dispatch by
  `Runner` while `processor_impl` still feeds `settings.job_engine` into it (one bridge line,
  valid because every in-repo runner name equals its engine). T4 renames the DB field and removes
  the bridge. Alternative: one L-sized commit touching 7 source and 6 test files.
- **`_check_content` before `runner.view` (T6 before T7).** The validation change is the one
  behaviour change; it gets its own commit and diff. It is also a precondition: windows strings
  fail today's dump-everything loop.
- **`runner_view(name, **paths)` test helper** patches `RUNNERS[name]._locations` the way
  `yaml_locations` patches `CLUSTER_VIEW`, so tests survive import-site moves. `yaml_locations`
  lives until T12.
- **Import-time surface grows:** `shared.runners.RUNNERS` and `yamlsettings.BACKUP_BASE_PATH`
  fail at import on an old yaml. The webapp imports `shared.runners` (T4), so the webapp
  container needs the new yaml too. No `MOUNTS_PATH` needed there: an unset variable gives an
  empty `DOCKER_HOST_VIEW`, which is only an error when the docker engine is selected.
- **Migration is independent of code order** (T5 can ship any time after T4), but deployment
  order is fixed: new yaml + code, then migration, before any quanting DAG runs.

## Phases and checkpoints

| Phase | Tasks | Checkpoint |
|---|---|---|
| 1 Runners exist | T1, T2 | A: `pytest shared` green, in-repo yamls still have `locations` |
| 2 Dispatch by runner | T3, T4, T5 | B: 9.1 grep empty, 9.5 reproducible, migration `--dry-run` runs |
| 3 Paths from the runner | T6, T7, T8 | C: 9.4 (windows QuantingEnv), 7.2/7.4/7.5 green |
| 4 `locations` dissolved | T9, T10, T11, T12 | D: 9.6, 7.7, 7.8 green, `mount.sh` fstab unchanged |
| 5 Docs | T13 | E: 9.2 grep clean, all three pytest suites and pre-commit green |

Each task is one commit, `pre-commit run --all-files` clean, three `pytest` suites green
(spec §4; 5 known `test_dags` failures without `AIRFLOW_HOME`).

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Sandbox DB holds `file_based` Settings, no in-repo yaml declares such a runner; those jobs fail with the 9.5 message after migration | Med | Migration prints distinct target names (2.8); decide per open question 1 before running it |
| `RUNNERS` built at import: a typo in a yaml takes down every container, incl. webapp | Med | Consistency test T2 covers the in-repo yamls; error names runner and key |
| T3 bridge line relies on runner name == engine | Low | Only for one commit; T4 removes it; in-repo yamls and `_test_` stub satisfy it |
| `mount.sh` sources `envs/${ENV}.env`, which holds passwords, into the shell | Low | Only `MOUNTS_PATH` is used; script already runs as the deploying user; document |
| `pre-commit`/`ty` on the frozen dataclass with `View[PurePath]` generic | Low | Same pattern as `path_views.py`; check in T1 |
| Windows paths joined from posix relative parts (`PureWindowsPath / PurePosixPath`) | Low | Already exercised by `test_path_views.py:53-77`; T1 adds layout-function cases |

## Open questions

1. `file_based` Settings in the sandbox DB: remap them via `_ENGINE_TO_RUNNER` to `slurm`, or add
   a `file_based` runner to `alphakraken.sandbox.yaml`? Plan assumes: decide after the `--dry-run`
   output, no in-repo yaml change.
2. T3/T4 split vs. one commit: plan assumes split (see decisions). Say so if you prefer one.
3. Spec 11.1, 11.2 stay assumed yes.
