# Path handling: design options

Commit: `0823eb52` (branch `path_refactoring`).
Companion to `PATH_HANDLING_OVERVIEW.md`, whose numbering (§2 ragged matrix, §6 problems) is referenced here.

Drivers: (a) refactor for clarity, (b) enable a second "cluster view" on a Windows machine,
(c) resolve paths differently depending on the nature of the quanting env.

## 1. The model underneath all options

Every path in the system is

```
view.base(location) / layout(entity)
```

Three axes are conflated today:

| Axis | Values |
|---|---|
| **location** | backup, output, settings, software, slurm, instruments, logs |
| **layout** | `<instrument>/<YYYY_MM>/<raw_file_id>`, `<project>/[<YYYY_MM>]/out_<raw_file_id>/<software_type>` |
| **view** | container, cluster, docker-host, SMB source (+ the new Windows one) |

The view is chosen *implicitly*, by which accessor a caller happens to import
(`get_internal_*` vs `get_path`). That is the root cause of §6.1-6.5.

## 2. Sizing facts

- The **cluster view** has 6 non-test call sites: `processor_impl.py:122,178,188,272`,
  `handler_impl.py:328`, `job_handler.py:23`.
- The **container view** has ~41 call sites across 12 files, but they are uniform and can keep
  their current API as thin wrappers.
- The **host view** has 1 site: `docker_job_handler.py:186`.

So the expensive-looking half (container) is the cheap half.

## 3. Answers that constrain the design

Obtained 2026-09-01:

- 3.1 The Windows target may be **any of**: a single box driven over SSH/WinRM, a Windows
  scheduler, or a drop-folder/file-based runner. The design must not assume a scheduler.
- 3.2 The Windows machine sees the data via **both** UNC paths (`\\server\share\...`) **and**
  mapped drive letters (`Z:\...`).
- 3.3 Scope for the first step: **D2 only**.

## 4. Options

### D1 - Extra yaml key, branch on engine

`locations.<location>.windows_absolute_path`; `get_path(key, engine)` picks.

- **Pro:** hours of work, no migration, no new concepts.
- **Con:** entrenches every problem in §6. A third view is a third key. `get_path` grows an
  engine argument that propagates to all callers. Still `Path` (posix flavor), so `\\host\share`
  is mangled on the Linux worker. Layout stays duplicated in 3 places.
- **Rating:** cheapest, and paid for twice.

### D2 - View table + layout module  (core refactor)

- `layout` module: pure, view-free, returns `(Locations, PurePosixPath)`. Single home for the
  raw-file and output layouts. Kills §6.3 and collapses the four relative anchors (§3.1-3.4)
  into "location + rel"; `RemovePathProvider`'s `Path()` sentinel becomes an honest `rel="."`
  on a named location.
- `views` module: `Locations` constants; `View(name, flavor, locations: dict[str, PurePath])` with
  `.resolve(location, rel)` and `.has(location)`. Instances `CONTAINER` (from `InternalPaths`),
  `CLUSTER` (from yaml `locations`), `HOST` (from `locations.general.mounts_path`).
  A missing location raises naming both view and location, so the §2 holes become explicit.

- **Pro:** the ragged matrix of §2 becomes data with a real error instead of a silent hole.
  A new view = one dict + a flavor. Layout defined once. Only the 6 cluster sites must move;
  container helpers stay as one-line wrappers so the 41 sites do not churn. `mount.sh` can later
  be generated from the same table (attacks §6.6).
- **Con:** new module and vocabulary; does not *statically* prevent view mixing; does not touch
  the DB-persisted views or the webapp TODOs (§5.12, §5.13).
- **Rating:** best clarity per unit of effort. Chosen as step 1.

### D3 - D2 + Runner objects

`runners:` in yaml (cf. the `alphakraken.example.part.yaml` sketch), made real:
`Runner{name, engine, view, ssh_connection_ids, job_script, location overrides}`;
`Settings.runner` replaces `Settings.job_engine`; `prepare_job` resolves all exported paths
through `runner.view`, while Airflow-side I/O keeps the container view.

- **Pro:** literally answers driver (c) - the nature of the quanting env *is* the runner.
  A Windows cluster becomes one yaml block (`flavor: windows`, own locations, own script, own SSH
  ids) plus a handler. Also fixes the global `cluster_ssh_*` prefix discovery
  (`common/utils.py:198`), which cannot address two clusters today.
- **Con:** DB migration `job_engine -> runner` plus webapp settings form; yaml schema grows;
  needs validation that a settings' runner exists.
- **Rating:** the piece that actually buys the second cluster. D2 without D3 gets clarity but
  not the feature.

### D4 - Typed view-tagged paths (on top of D2)

`ViewPath(view, path)` or a `NewType` per view; `QuantingEnv` fields annotated by view.

- **Pro:** kills §6.1 statically; the `internal_` naming convention becomes redundant;
  `_check_content`'s whitelist-by-field-name disappears.
- **Con:** the type leaks at every pydantic / mongoengine / `str()` / filesystem boundary.
  Ceremony out of proportion to 6 cluster call sites.
- **Rating:** skip. Revisit beyond ~4 views.

### D5 - Relative-on-the-wire

Export only location-relative paths plus per-location base env vars; the job script joins.
DB stores relative only; the webapp resolves for display.

- **Pro:** exactly one view exists in Python. Fixes the persisted-view problem (§5.12), the
  6 webapp TODOs (§5.13), and `RawFile.backup_base_path` disappears. A new view then costs
  zero Python.
- **Con:** joins move into bash/PowerShell; `{RAW_FILE_PATH}` placeholder semantics change
  *visibly* for users, so existing settings need migrating; DB migration. Largest blast radius.
- **Rating:** plausibly the right end state, definitely the wrong first step.

## 5. Recommendation

D2 now, D3 next, D4 never (until views multiply), D5 as a separate later decision.

## 6. Constraints any Windows view must satisfy (independent of the option chosen)

- 6.1 **Validation blocks Windows paths outright.** `check_for_malicious_content`
  (`shared/validation.py:14`) allows only `[A-Za-z0-9\-_+./ ]`. Every `\`, `:` and `\\` fails,
  so `_check_content` (`processor_impl.py:280-300`) rejects every Windows path today.
  Fix by validating the *relative* part strictly and treating the base as admin-controlled yaml,
  not user input. Do **not** simply add `\` and `:` to the allowed set - that weakens injection
  defence for the bash runner.
- 6.2 **Path flavor.** Foreign-view paths must be built with `PureWindowsPath` /
  `PurePosixPath`, never `Path`: on the Linux worker `Path("\\\\srv\\share")` is one filename,
  not a UNC location. The view must own its flavor, and yaml locations must stay opaque strings so
  that both UNC and drive letters (§3.2) survive.
- 6.3 **The job script is bash-only.** `submit_job.sh` uses `sbatch`, `module load`, `md5sum`.
  A Windows runner needs its own script and submitter. That is a Runner concern, not a path
  concern - an argument for D3.
- 6.4 **`mount.sh`, `docker-compose.yaml` and `InternalPaths` agree by convention only** (§6.6),
  cf. the production `backup` mount-depth finding in `BOYSCOUT_20260901_081142.md`. Once the
  container view is a table, a test comparing it to `docker-compose.yaml` is ~20 lines.
