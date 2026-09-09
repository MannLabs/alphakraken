# Add the `direct_ssh` job engine

## Context

A deployment should be able to run quanting jobs on a plain machine reachable over SSH — no Slurm,
no Docker daemon, no file-queue watcher. The `refactor_introduce_runners_XI` branch built exactly
that seam: `SPEC.md` §2.5 states "Adding the SSH handler later is: one `JobEngines` constant, one
factory branch, one module", and `SPEC.md` §10 deliberately left the handler out of scope. This
change fills it in.

### About "bring the removed engine back first"

The removed engine was `GenericJobHandler` / `JobEngines.GENERIC`, deleted in `d6642656`
(2026-08-26), file `airflow_src/plugins/jobs/_experimental/generic_job_handler.py`. It was a
35-line stub that could never run a job:

- `start_job` ran `command = "sleep 60"` with a `TODO: replace with path + job_script_name`, and
  `del`'d the environment, the script name and the year-month folder.
- `get_job_status` was `ps -p $PID`, so only RUNNING vs. COMPLETED — a failed job reported
  COMPLETED.
- `get_job_result` returned `(status, 0)` with `# how to get the run time?`.

It also cannot be restored as a first step, because every interface it used has since changed:
`start_job(job_script_name, environment, year_month_folder)` → `start_job(quanting_env)`,
`ssh_execute(command)` → `ssh_execute(command, ssh_connection_id_prefix)`, and the factory now
takes a `Runner` instead of an engine string. **So there is no revert commit.** What carries over
from the old engine is its idea (SSH + background process + PID), and the two decisions below fix
the two things it got wrong. The old file is quoted in the git history if a reviewer wants it:
`git show d6642656`.

### Decisions taken (2026-09-08)

- Engine name `direct_ssh`; `JobEngines.DIRECT_SSH`, `DirectSSHJobHandler`,
  `jobs/direct_ssh_job_handler.py`. (`SPEC.md`:92,205 anticipates the name `ssh`; the comment at
  `SPEC.md`:92 gets corrected.)
- The job is the already-resolved `custom_command`, run directly — the same thing the docker
  engine uses as its container command. No script on the remote host, no `submit_job.sh`.
  Consequence: `software_type: alphadia` does not work (its conda logic lives in
  `submit_job.sh`, and `_prepare_custom_command` returns `""` for alphadia,
  `processor_impl.py:203-208`), so the engine is restricted to custom software exactly like
  docker is.
- Failure is detected: the job's exit code is captured into a file in the output folder.
- `os: linux`, `macos` and `windows` are all supported.

## Design

### Where the work happens

`processor_impl.py:395-397` creates the output folder through `AIRFLOW_CONTAINER_VIEW` and only
then calls `start_job`. The handler therefore writes a **launcher script into the existing output
folder locally** (the output bind is `rw`, `docker-compose.yaml:458`) and the SSH call only starts
it. The runner sees the same folder as `view.output`.

Why a file and not one long inline command: the env exports plus the command plus the exit-code
capture is the part where quoting through paramiko → OpenSSH → `sh`/`cmd.exe` breaks, and it is
the part worth unit-testing without any SSH mock. Status and result stay single inline commands.
This relies on the runner's `view.output` and the worker's `AIRFLOW_CONTAINER_VIEW` output being
the same directory — an assumption the pipeline already makes, since `check_job_result` and the
metrics tasks read that folder from the worker after the job.

Quoting is safe because `_check_content` (`processor_impl.py:311-325`) validates
`settings.config_params` through `check_for_malicious_content(..., allow_spaces=True)`, whose
allowed set is `a-zA-Z0-9-_+./` plus space (`shared/validation.py:5,14-15`). **`custom_command`
can therefore never contain a quote, `;`, `$` or a backtick.** State this as a precondition in
the module docstring — it is what makes single-quoting the command sound.

### The three commands

One status command serves both `get_job_status` and `get_job_result`: it prints a single line
`<STATE> <ELAPSED_SECONDS>`, so `get_job_status` takes the first token and `get_job_result` both.
Every command ends in an `echo` — ❗️`ssh_execute` treats empty stdout as a failure and retries
with `sleep(60 * n)` up to `max_tries=30` (`ssh_utils.py:38,75`), i.e. a silent command hangs the
task for hours.

Constants (module-level, `ALL_UPPERCASE`): `LAUNCHER_SCRIPT_STEM = "_alphakraken_job"`,
`EXIT_CODE_FILE_NAME = ".alphakraken_exit_code"`, `LOG_FILE_NAME = "log.txt"` (same name the
docker handler writes, `docker_job_handler.py:48`).

**posix launcher** `_alphakraken_job.sh`:

```sh
export RAW_FILE_PATH="…"        # every non-underscore QuantingEnv key, cf. below
cd "<runner view.output>/<relative_output_path>"
START=$(date +%s)
<custom_command> > log.txt 2>&1
CODE=$?
echo "$CODE $(($(date +%s) - START))" > .alphakraken_exit_code
```

**posix start** — job id is the PID:

```sh
nohup sh "<output>/_alphakraken_job.sh" > /dev/null 2>&1 &
echo $!
```

**posix status**:

```sh
if [ -f "<output>/.alphakraken_exit_code" ]; then
  read CODE ELAPSED < "<output>/.alphakraken_exit_code"
  [ "$CODE" = 0 ] && echo "COMPLETED $ELAPSED" || echo "FAILED $ELAPSED"
elif kill -0 <pid> 2>/dev/null; then echo "RUNNING 0"
else echo "FAILED 0"; fi
```

**Windows** uses a `.cmd` launcher, not `.ps1`, so no ExecutionPolicy is involved, and PowerShell
only for starting and inspecting the process. Every SSH command is prefixed
`powershell -NoProfile -Command "…"`, which works whether the OpenSSH default shell is `cmd.exe`,
PowerShell or git-bash.

```bat
@echo off
set "RAW_FILE_PATH=…"
cd /d "<output>"
<custom_command> > log.txt 2>&1
echo %ERRORLEVEL% 0 > .alphakraken_exit_code
```

The trailing `0` is the elapsed time: ❗️**Windows reports `time_elapsed = 0`.** Batch cannot do
portable time arithmetic and it is not worth a second interpreter in the launcher. Precedent:
`file_based_job_handler.get_job_result` does the same (`_experimental/file_based_job_handler.py:132-134`).
Documented as a known limitation, not hidden.

Windows start uses `(Start-Process cmd -ArgumentList '/c','<launcher>' -PassThru -WindowStyle Hidden).Id`;
Windows status uses `Test-Path` / `Get-Content` / `Get-Process -Id <pid> -ErrorAction SilentlyContinue`
to print the same `<STATE> <ELAPSED>` line.

### Keeping the handler OS-agnostic

Two small private dialect classes in the same module (`_PosixDialect`, `_WindowsDialect`), each
with `launcher_file_name`, `launcher_script(...)`, `start_cmd(...)`, `status_cmd(...)`, selected
from `runner.os` via a dict. `DirectSSHJobHandler` itself contains no OS branching: it writes the
file, calls `ssh_execute(cmd, self._ssh_connection_id_prefix)`, and parses the one-line contract.
Reuse `SlurmSSHJobHandler._create_export_environment_cmd`'s rule — skip keys starting with `_`
(`slurm_ssh_job_handler.py:147-152`, mirrored by `docker_job_handler._exported_environment`) —
but implement it per dialect, since `export K="V"` and `set "K=V"` differ.

### State mapping

Only `COMPLETED`, `FAILED` and `RUNNING` from `common.keys.JobStates` are produced. `PENDING` is
never produced (no queue), so `WaitForJobStartSensor` passes immediately — fine,
`WaitForJobFinishSensor` also waits on `PENDING` (`ssh_sensor.py:85-89`). `check_job_result`
(`processor_impl.py:479-483`) understands all three.

Known limitations to record in the module docstring:
- `_SLURM_MEM` / `_SLURM_CPUS_PER_TASK` / `_SLURM_TIME` are not honored — the engine has no
  resource control. Concurrency is bounded only by `cluster_slots_pool`, which already gates all
  runners (`acquisition_processor.py:97`).
- PID reuse: if the launcher is killed before writing the exit-code file and the PID is recycled,
  the status stays `RUNNING`. The exit-code file is checked first, so this needs the launcher
  itself to die.

## Files

**New**

- `airflow_src/plugins/jobs/direct_ssh_job_handler.py` — `DirectSSHJobHandler(output_view, runner_os, ssh_connection_id_prefix)`
  plus the two dialects.
- `airflow_src/tests/plugins/jobs/test_direct_ssh_job_handler.py`

**Changed**

- `shared/keys.py:66-71` — `DIRECT_SSH: str = "direct_ssh"` in `JobEngines`.
- `shared/runners.py:42-47` — `_REQUIRED_LOCATIONS[JobEngines.DIRECT_SSH] = _JOB_LOCATIONS`
  (❗️mandatory: line 89 is an unguarded `_REQUIRED_LOCATIONS[self.engine]`, so omitting it makes
  `import shared.runners` die with a bare `KeyError`), and
  `_ENGINES_USING_SSH = (JobEngines.SLURM, JobEngines.DIRECT_SSH)`.
- `airflow_src/plugins/jobs/job_handler.py:18-57` — one factory branch, lazy import, mirroring the
  slurm branch: reuse the existing `assert runner.ssh_connection_id_prefix is not None`
  ("guaranteed by shared.runners", lines 24-26) and pass
  `runner.view.resolve(Locations.OUTPUT)` and `runner.os`.
- `airflow_src/plugins/sensors/ssh_utils.py:91-108` — extend `_get_fake_ssh_response` so
  `debug_no_cluster_ssh=true` works for this engine: a command containing
  `LAUNCHER_SCRIPT_STEM` returns a numeric PID, one containing `EXIT_CODE_FILE_NAME` returns
  `"COMPLETED 1"`. Without this, the local-stack checkpoints in `tasks/todo.md` cannot exercise
  the engine.
- `webapp/service/settings_validation.py:14-21` — the docker-only-custom rule becomes a tuple of
  engines that only support `SoftwareTypes.CUSTOM`, now `(DOCKER, DIRECT_SSH)`; keep the message
  naming the offending engine.
- `webapp/pages_/settings.py:322` — the `software` help text mentions the docker engine; add one
  clause that `direct_ssh` needs a custom executable too.
- `envs/alphakraken.local.yaml` — a **live** `direct_ssh` runner in the `runners:` block
  (`os: linux`, its own `ssh_connection_id_prefix`, the four job locations), in the style of the
  existing comment-rich entries. It is validated at import and by `test_runners_are_valid`
  (`shared/tests/test_deployment_paths.py:129-133`), so it cannot rot. It also appears in the
  webapp runner dropdown for local deployments and will fail at job start with an SSH connection
  error until an Airflow connection with that prefix exists — expected for the local env.
  `sandbox` and `production` yamls are left alone.
- `SPEC.md:92,205-206` — correct the two places that name the future handler `ssh`.
- `README.md:19` — the compute-environment list still advertises "generic SSH (experimental)",
  a leftover of the engine removed in `d6642656`; replace it with `direct_ssh`.
- `docs/deployment.md` — a short section next to "Standalone deployment without a cluster"
  (`:329-380`): prerequisites (SSH connections under the runner's prefix, cf. `:280-300`; the
  output and backup folders mounted at the paths in the runner's `view`; custom software only;
  no resource limits; `time_elapsed` is 0 on Windows).

**Deliberately not changed**

- `shared/yamlsettings.py:76-113` (the `_test_` runner stub). Adding a fourth stub runner would
  break the three exhaustive assertions at `shared/tests/test_runners.py:200-206`,
  `shared/tests/test_yamlsettings.py:94-124` and
  `airflow_src/tests/plugins/jobs/test_job_handler.py:106` for no gain — the handler tests build a
  `Runner` directly via the existing `_runner()` helper (`test_job_handler.py:27-35`).
- `shared/_migrations/**` — `_migrate_job_engine_to_runner.py` derives its map from
  `JobEngines.get_values()` (line 33), so a new engine needs nothing; `_convert_alphakraken_yaml.py`
  is a one-shot 0.10.0 tool that hardcodes one slurm runner by design.

## Tests

- `test_direct_ssh_job_handler.py` — the substance:
  - launcher script content for `linux` and for `windows`: underscore-prefixed env keys absent,
    every other key present, the command and the exit-code capture in place;
  - `start_job` writes the launcher into the output folder and returns the last stdout line as the
    job id; a non-numeric last line raises;
  - `start_job` raises naming the folder when the local output folder does not exist (mirrors
    `docker_job_handler.py:100-102`);
  - `get_job_status` / `get_job_result` over a mocked `ssh_execute` for all four status lines:
    `COMPLETED 42`, `FAILED 7`, `RUNNING 0`, and a garbage line;
  - every generated command ends in an `echo` (the empty-stdout constraint above);
  - the quoting precondition: a `custom_command` with a space survives, one with a quote is not a
    case that can arise (assert the validator's allowed set, not the handler).
- `airflow_src/tests/plugins/jobs/test_job_handler.py` — factory routes `direct_ssh` to
  `DirectSSHJobHandler`.
- `shared/tests/test_runners.py` — a `direct_ssh` runner without `ssh_connection_id_prefix` is
  rejected; one without the `slurm` view key is accepted; one with `os: windows` is accepted.
- `webapp/tests/service/test_settings_validation.py` — a `direct_ssh` runner rejects
  `software_type: alphadia` and accepts `custom`.
- `airflow_src/tests/plugins/sensors/test_ssh_utils.py` — the two new fake responses.

## Verification

1. `pre-commit run --all-files` — ❗️expect the pre-existing `ty` error in `msqc-extractor/main.py:220`
   (recorded in `BOYSCOUT_2026-09-03.md`); nothing new.
2. `pytest shared`, `pytest webapp`,
   `pytest airflow_src --ignore=airflow_src/tests/plugins/jobs/test_docker_job_handler.py`
   (the 5 known `test_dags` failures without `AIRFLOW_HOME` excepted).
3. `ENV_NAME=local python -c "import shared.runners"`: the new `direct_ssh` runner in
   `envs/alphakraken.local.yaml` imports clean; temporarily drop its `ssh_connection_id_prefix`
   and confirm the import error names the runner and the key.
4. Local stack, `debug_no_cluster_ssh=true`: a settings entry on a `direct_ssh` runner with custom
   software, trigger the quanting DAG, confirm `prepare_job` exports `_RUNNER_NAME: direct_ssh`,
   the launcher script appears in the output folder with the expected content, and the DAG reaches
   `check_job_result`.
5. Real end-to-end against one Linux host: add an Airflow connection under the runner's prefix,
   run a job with `software` a small executable, confirm `log.txt` and
   `.alphakraken_exit_code` in the output folder, a nonzero exit code surfacing as `FAILED`, and a
   plausible `time_elapsed` metric. ❗️No Windows host is available here, so the Windows dialect
   ships verified by unit tests only — say so in the PR.

## Boyscout notes (report in `BOYSCOUT_<timestamp>.md`, do not commit)

- `shared/runners.py:89` — `_REQUIRED_LOCATIONS[self.engine]` is an unguarded lookup inside a
  pydantic validator; an engine added to `JobEngines` alone crashes the import with a bare
  `KeyError` instead of a named `ValueError`.
- `airflow_src/plugins/jobs/_experimental/file_based_job_handler.py:9,71` — claims the output bind
  must be changed from `ro` to `rw`; `docker-compose.yaml:458` already binds it `rw`.
- `airflow_src/plugins/sensors/ssh_utils.py:68` — the slurm-specific retry string
  `"Batch job submission failed"` now sits in a generic helper used by every SSH engine.

## Note for the author

The branch still has the Phase 2-5 human-review checkpoints unticked in `tasks/todo.md`; this
change stacks a new engine on an unreviewed refactor. Starting point is a clean tree at
`a861610d`.
