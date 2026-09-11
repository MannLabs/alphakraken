# Implementation Plan: Named runners (D3-lite)

Spec: `SPEC.md` at `d24994dd`. Task list: `tasks/todo.md`.
Merged from the first plan (`d2a7fca7`) and `tasks/alternative_plan.md`.

## Overview

Replace the single `CLUSTER_VIEW` / `Settings.job_engine` pair by a list of named runners, each
with engine, os, view and SSH prefix. Dissolve the yaml `locations` block into `runners[].view`,
`mounts` and `backup.backup_base_path`. Pure refactoring except validation moving to the relative
parts (spec 1.2.2).

## Strategy

The spec's end state has no fallback, but the path there does not need to be one commit.
Every task leaves the repo deployable with the in-repo yamls:

1. Four independent foundations land first (`MOUNTS_PATH` env, `backup_base_path`,
   `_check_content`, `runners:`). Nothing consumes `RUNNERS` yet.
2. Job dispatch switches to runner names (DB rename).
3. SSH and path resolution switch to the runner; `CLUSTER_VIEW` goes with its last reader.
4. `locations` is deleted from the yamls when no code reads it.

Intermediate yamls carry `locations` and `runners` side by side for a few commits. That is
duplication, not compatibility: no missing key is tolerated at any point.

## Dependency graph

```
T1 DOCKER_HOST_VIEW from MOUNTS_PATH env      independent
T2 backup.backup_base_path                    independent
T3 _check_content explicit list               independent
T4 shared/runners.py + runners: yaml          needs T2 (consistency assertion backup_base_path == slurm view.backup)
        │
T5 QuantingEnv.runner, factory takes Runner   needs T4 (+ T3: adds `runner` to the strict list)
        └── T6 Settings.runner, webapp        needs T5
                └── T7 migration              needs T6
T8 slurm base dir + SSH prefix from runner    needs T5
T9 prepare_job via runner.view, CLUSTER_VIEW deleted   needs T2, T6, T8 (last readers gone)
T10 locations -> mounts, mount.sh, keys       needs T1, T9
T11 docs, comment sweep, §9 greps             needs all
```

## Architecture decisions

- **One transitional line, in `processor_impl` (T5).** T5 introduces `QuantingEnv.runner` and a
  factory that takes a `Runner`; `processor_impl` feeds `runner=settings.job_engine` for one
  commit. Valid because every in-repo runner name equals its engine. T6 removes it. Rejected
  alternative: passing the runner name as `engine=` into an engine-keyed factory, which leaves
  the factory wrong for a commit.
- **DB rename split (T5, T6, T7)** instead of one 12-file commit. The rename changes an exported
  env var name; the diffs should be reviewable on their own.
- **`_check_content` first (T3)**, without `runner`; T5 adds it to the list. The validation
  change is the one behaviour change and gets its own diff. Windows strings must pass it before T9.
- **SSH prefix plumbed in one step (T8)** once the factory has a `Runner`. No commit passes a
  module constant into a freshly added parameter.
- **`CLUSTER_VIEW` deleted in T9**, the commit that removes its last reader. `YamlKeys.LOCATIONS`
  lingers until T10, unread.
- **`runner_view(name, **paths)` test helper** (T9) patches `RUNNERS[name].view._locations` the way
  `yaml_locations` patched `CLUSTER_VIEW`; `yaml_locations` is deleted in the same commit.
  `test_job_handler.py` constructs `Runner` objects directly (T8) and needs no helper.
- **Import-time surface grows:** `shared.runners.RUNNERS` (T4) and `yamlsettings.BACKUP_BASE_PATH`
  (T2) fail at import on an old yaml. The webapp imports `shared.runners` from T6 on, so the
  webapp container needs the new yaml too. It needs no `MOUNTS_PATH`: an unset variable yields an
  empty `DOCKER_HOST_VIEW`, an error only when the docker engine is selected.
- **Import-time raises are tested through the builder functions** (`_build_runners`, a
  `_read_backup_base_path`-style helper), not by reloading modules. One reload-based test at most
  for 9.6.

## Phases and checkpoints

| Phase | Tasks | Checkpoint |
|---|---|---|
| 1 Foundations | T1, T2, T3, T4 | 1: local yaml loads with `runners`, `backup_base_path` and old `locations` side by side |
| 2 Dispatch by runner | T5, T6, T7 | 2: 9.1 grep empty; local stack: settings with runner `slurm`, DAG run with `debug_no_cluster_ssh`, `_RUNNER` in the `prepare_job` XCom |
| 3 Paths from the runner | T8, T9 | 3: 9.4 by test; `grep CLUSTER_VIEW` empty; `prepare_job` XCom unchanged vs. checkpoint 2 |
| 4 Yaml final shape | T10 | 4: no `locations` key; local stack boots; `mount.sh` fstab line unchanged |
| 5 Docs | T11 | 5: 9.2 grep clean; TODO count unchanged; three suites and pre-commit green |

Each task is one commit, `pre-commit run --all-files` clean, three `pytest` suites green
(spec §4; 5 known `test_dags` failures without `AIRFLOW_HOME`).

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| T9 changes an exported string by accident | High | The existing full-dict assertion in `test_create_quanting_env` is the 7.3 regression; it changes only in T5 (`_JOB_ENGINE` -> `_RUNNER`) |
| T6 fan-out misses a test fixture building `Settings` | Med | `grep -rn job_engine` incl. tests is the gate; `pytest -x` per suite |
| `RUNNERS` / `BACKUP_BASE_PATH` at import: a yaml typo takes down every container incl. webapp | Med | Yamls and stub change in the same commit; consistency test T4 covers the in-repo yamls |
| `mount.sh` sourcing `envs/${ENV}.env` pulls passwords and `ENV_NAME` into the shell | Med | Source in a subshell, export only `MOUNTS_PATH`; manual fstab diff before/after |
| Sandbox DB holds `file_based` Settings, no in-repo yaml declares such a runner | Med | Migration prints distinct target names; decide per open question 1 before running it |
| T5 bridge relies on runner name == engine | Low | One commit; named in the commit message; T6 removes it |
| `PureWindowsPath / PurePosixPath` join | Low | Already exercised by `test_path_views.py:53-77` |

## Open questions

1. `file_based` Settings in the sandbox DB: remap via `_ENGINE_TO_RUNNER` to `slurm`, or add a
   `file_based` runner to `alphakraken.sandbox.yaml`? Plan assumes: decide after the `--dry-run`
   output, no in-repo yaml change.
2. Spec 11.1, 11.2 stay assumed yes.
