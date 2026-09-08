---
name: deploying-alphakraken
description: Plans a first sandbox or production AlphaKraken deployment on custom infrastructure as a walking skeleton — one instrument, no optional components — with checkpoints at file discovery, backup, and the first cluster job. Interviews the operator about available machines, compute, storage and instruments, edits the config files accordingly, and writes a phased DEPLOYMENT_CHECKLIST.md grouped per machine, with the exact commands to run and a success check after each one. Use when setting up AlphaKraken on new hardware ("how do I deploy this on our machines", "set up AlphaKraken here", "deploy to our cluster"). Not for local development (see docs/development.md) and not for day-2 ops (see docs/instruments.md, docs/maintenance.md).
---

# Deploying AlphaKraken on custom infrastructure

`docs/deployment.md` is the source of truth. This skill does not repeat it — it decides **which**
of its steps apply to *this* operator's infrastructure, in **which order**, on **which machine**,
and **how to tell each step worked**.

Scope: `sandbox` and `production` only. A `local` setup is development, not deployment — point at
`docs/development.md#local-testing` and stop.

## Rules

1. **Walking skeleton first.** Plan the smallest end-to-end pipeline that can process one file,
   then grow it. Never plan the full installation in one pass — see *Phase plan*.
2. **Link, never restate.** A checklist item is one imperative line + the machine + the command or
   file + a link `docs/deployment.md#anchor`. If you find yourself explaining *how* a documented
   step works, delete the explanation and link instead.
3. **Every action gets a check.** No item is done because a command exited 0. Give the observable
   proof as an indented `Check:` line — see *Standard checks* below.
4. **Commands are quoted verbatim** in the checklist (`./compose.sh …`, `./mount.sh …`,
   `docker build …`), so the operator can copy-paste. Substitute their real `ENV`, hostnames,
   instrument ids and profiles — no placeholders they have to resolve themselves.
5. **You edit config; the operator runs commands.** You may edit `envs/${ENV}.env`,
   `envs/alphakraken.${ENV}.yaml`, `envs/.env-airflow`, `envs/.env-mongo`, `docker-compose.yaml`,
   `misc/nginx.conf`. You never run `compose.sh`, `mount.sh`, `docker`, `sudo`, or touch
   `/etc/fstab`.
6. **Passwords by environment.** `sandbox` holds no valuable data: set simple, obvious passwords
   yourself (e.g. `sandbox`) so the operator is not blocked, and say so. `production`: never invent
   one — write `SET_STRONG_PASSWORD_NO_SPECIAL_CHARS` and make filling it a checklist item.
   Constraints in `#additional-steps-required-for-initial-sandboxproduction-deployment`.
7. If an answer maps to no documented path, say so plainly instead of improvising a deployment.

## Step 1 — read

Read `docs/deployment.md` in full, the comments in `envs/alphakraken.local.yaml` (the commented
reference for the yaml) and the `*_HOST` comments in `envs/sandbox.env`. Skim
`README.md#system-requirements`. Do not start the interview before that.

## Step 2 — interview

Three `AskUserQuestion` batches, in this order. Skip any question already answered in the prompt.

**Batch A — topology**
- Target environment: `sandbox` or `production` (or a custom name modelled on one of them).
- Machine layout: one machine for everything / two (dbs + rest) / three or more
  (dbs, infrastructure, workers) / already-running instance, adding a machine.
- Which OS account owns the checkout and runs `compose.sh` on each machine — and whether Docker and
  `python3` are installed. Reject `root`, see *Push back when*.
- Which **single** instrument to start with, and its vendor (thermo / bruker / sciex). Ask for the
  full list too, but only to plan phase 4 — the skeleton gets one.

**Batch B — compute and storage**
- Where quanting jobs run: Slurm cluster over SSH / containers on the worker host (`docker` engine)
  / nothing yet (discovery and backup only).
- Shared file system: CIFS shares reachable from the worker host / already mounted natively /
  none. Also: is it reachable from the compute nodes under a *different* path? (the "cluster view").
- Raw file backup target: pool folder (`local`) or S3.
- Which phase they want to reach now (see *Phase plan*), if the goal is a partial deployment.

**Batch C — accounts and what comes later** (multi-select where sensible)
- Two service accounts available (`kraken-write` with write access to backup, `kraken-read`
  read-only access to backup, cf. `#required-users`)? If only one exists, flag the least-privilege loss.
- Which of these are wanted **eventually**: nginx reverse proxy with TLS + basic auth ·
  Slack/Teams alerting · nightly MongoDB backups · S3 upload worker · MCP server / REST API (both
  come with the `infrastructure` profile anyway). None of them enter the skeleton — they are phase 6.

## Phase plan

The checklist is organised by phase, and each phase ends in one observable checkpoint. Do not let
the operator start a phase before the previous checkpoint holds.

| Phase | Content | Checkpoint |
|---|---|---|
| 0 | Prerequisites, accounts, per-host env wiring, config edits | `compose.sh ps` and the db ports answer from every machine |
| 1 | `dbs` → `infrastructure` → `workers` for **one** instrument, all its mounts. Unpause `instrument_watcher.<id>` only | **File discovery**: the watcher logs list real files and they appear in the webapp |
| 2 | Unpause `acquisition_handler.<id>` | **Backup**: a file is copied to the backup location and its checksum matches |
| 3 | The first quanting software: **MSQC**, then unpause `acquisition_processor.<id>` | **Compute reachable**: one job runs on the cluster and `msqc__*` metrics appear in the webapp |
| 4 | AlphaDIA (or the real analysis software), then the remaining instruments | metrics for the real software; each new instrument reaches checkpoint 2 |
| 5 | File mover, then file remover — the write operations on the instruments | files move, then old files are purged, with free space as expected |
| 6 | nginx, alerting, S3, db backups, anything else from Batch C | each component's own check |

**Why MSQC before AlphaDIA** (phase 3): it needs no spectral library, no fasta, no config file, no
conda environment and no `mono` — so a failure points at the connection, not at the analysis. It
exercises everything the wiring needs: the SSH connection, `submit_job.sh`, the cluster view paths,
the pool being visible from the compute node, the output write-back and metrics ingestion. Recipe
in `#setup-a-custom-software-on-the-cluster`; for a runner with the `docker` engine use
`#standalone-deployment-without-a-cluster` instead.

### Partial deployments

Keep the phase order and drop what does not apply. Say explicitly in the checklist which phase the
plan stops at, and what is therefore *not* running yet.

- No cluster → phase 3 uses the `docker` engine instead, which is the documented msqc path.
- Discovery and backup only → stop after phase 2; leave `skip_quanting: true` and say so.
- Adding a machine or an instrument to a running instance → phases 0, 1, 2 for that scope only.
- Instrument that must not be written to → its `file_move_delay_m: -1` and `min_free_space_gb: -1`
  stay set, and phase 5 is skipped for it, not postponed.

## Step 3 — decide

Map answers to steps. Always in the checklist:

| Step | Machine | Phase | Doc anchor |
|---|---|---|---|
| Install Docker + `python3`, clone repo at the same commit | every | 0 | `#setting-up-new-alphakraken-instance-workers-andor-infrastructure` |
| `echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow` — per machine, never copied | every | 0 | same |
| `export ENV=<env>` in every shell that runs `compose.sh`/`mount.sh` | every | 0 | `#deployment` |
| `./misc/bootstrap_airflow.sh --init` (once, ever — db init + Pools + Variables) | db host | 1 | `#one-time-initialization-of-airflow-infrastructure` |
| Review the bootstrapped Pools and Variables; size `cluster_slots_pool` to the real capacity | UI | 1 | `#setup-required-pools` |
| Airflow connection `cluster_ssh_connection` (real or dummy) | UI | 1 | `#setup-ssh-connection` |

Conditional:

| Answer | Adds |
|---|---|
| one machine | `dbs`, then `infrastructure`, then `workers` profiles on that host; `*_HOST` may stay at the compose service names |
| ≥2 machines | per-host env wiring (see below); bring-up in dependency order (dbs → infrastructure → workers); NTP time sync (`#additional-steps-required-for-initial-sandboxproduction-deployment`) |
| custom env name | copy `envs/sandbox.env` and `envs/alphakraken.sandbox.yaml` to the new name |
| CIFS shares | `sudo apt install cifs-utils`; `MOUNTS_PATH` **absolute**; create the mount targets first (`mount.sh` will not mount into a missing folder); one `./mount.sh <entity> fstab` per entity, pasted into `/etc/fstab` with passwords (`#set-up-pool-bind-mounts`). `airflow_logs` on every machine that runs an airflow container; `backup`, `output` and the skeleton instrument on worker machines. All of them in phase 1 — a missing bind mount target is silently created as an empty local folder, so never defer one |
| debugging / first try | offer `./mount.sh <entity> mount` instead (`#alternative-non-persistent-mounts`) |
| Slurm | cluster dir + `submit_job.sh` with adapted `partition`/`nodelist`; `runners[].view` paths = *cluster view* (phase 1); msqc wrapper in the `software` view (phase 3, `#setup-a-custom-software-on-the-cluster`); AlphaDIA env named `alphadia-<version>` + `mono` (phase 4) (`#on-the-cluster`, `#setup-alphadia-on-the-cluster`) |
| `docker` engine | `INSTALL_DOCKER_ENGINE=true` + `DOCKER_GID`; `docker build -t alphakraken-msqc msqc-extractor`; dummy ssh connection + Airflow var `debug_no_cluster_ssh=true`; size `cluster_slots_pool` to the host; settings entry in the webapp (`#standalone-deployment-without-a-cluster`) |
| no quanting yet | `skip_quanting: true` per instrument in the yaml; stop after phase 2 |
| S3 backup | phase 6: `backup.backup_type: s3`, `aws_default` connection, `s3_upload_pool`, `./compose.sh --profile s3 up --build -d` (`#s3-configuration-optional`) |
| each further instrument | phase 4: yaml block + worker service in `docker-compose.yaml` + mount + unpause DAGs (`docs/instruments.md`) |
| nginx | phase 6: IPs in `misc/nginx.conf`, `htpasswd` file, cert or `nginx_no_ssl.conf`, `./compose.sh up nginx --build --force-recreate -d` (`#url-redirect`) |
| alerting | phase 6: `general.notifications.*` in the yaml (`#monitoring--alerting`) |
| db backups | phase 6: `misc/backup_db.sh` path + cron (`#automated-mongodb-database-backups`) |

### Per-host env wiring

`envs/${ENV}.env` is a **per-machine** file, not a shared one. Copying one machine's copy verbatim
to the others is the most common deployment bug: `MONGO_HOST=mongodb-service` resolves only inside
the compose network of the machine that actually runs the `dbs` profile.

Decide and state, per machine, in the checklist:
- `MONGO_HOST` / `POSTGRES_HOST` / `REDIS_HOST` — the db machine's hostname or IP on every machine
  that does *not* run the `dbs` profile. Simplest correct choice: use that address on **all**
  machines, provided the db machine resolves its own name; the service-name variant is valid only
  on the db machine itself. Comments in `envs/sandbox.env`.
- `MOUNTS_PATH` — absolute, and may legitimately differ per machine.
- `envs/.env-airflow` — generated on each machine (`id -u` differs); never copy it.
- `envs/.env-mongo` — needed only on the db machine.
- Ports — only meaningful on the machine that serves them.

Every value that differs between machines gets its own checklist item under that machine, plus a
connectivity check.

### No writes to the instrument before phase 5

Phases 1–4 must not modify anything on the acquisition PC. Per instrument in
`envs/alphakraken.${ENV}.yaml`: `file_move_delay_m: -1` (no file moving) and
`min_free_space_gb: -1` (no file removing) — semantics in the yaml comments — and the
`file_mover.*` / `file_remover.*` DAGs stay paused. Copying to backup (phase 2) only reads.

Carry phase 5 in the checklist as an explicit "not yet" item, so nobody assumes purging works.

### Push back when

- **`root` as the OS account** owning the checkout or running `compose.sh`. `AIRFLOW_UID` would be
  `0`, so every container writes root-owned files into the mounts and logs. Insist on an
  unprivileged account (the operator's own, or a dedicated `kraken` login) with docker access.
  Same for the CIFS/pool accounts: use the dedicated `kraken-read`/`kraken-write` service accounts,
  not a root or admin account (`#required-users`).
- **All instruments at once**, or AlphaDIA before a cluster job has ever run. Offer the phase plan
  and name what the shortcut costs: a failure then has a dozen possible causes instead of one.
- Slurm chosen but no shared file system reachable from both sides — the cluster view cannot be
  resolved; this is not a supported deployment.
- `docker` engine on a multi-machine production setup — mounting the docker socket into workers is
  root on the host (`#standalone-deployment-without-a-cluster`).
- Relative `MOUNTS_PATH` — breaks fstab lines and the docker job engine.
- One shared account instead of `kraken-read`/`kraken-write` — the backup pool loses its read-only
  guard.

## Step 4 — edit config

Make the edits from Rule 5, one file at a time, and list each in the checklist under
*Config already edited* so the operator reviews rather than repeats them. Everything that must be
edited is enumerated in `#summary` — check your edits against that list before moving on.

Configure **only the skeleton**: one instrument in the yaml, one worker service in
`docker-compose.yaml`, one runner. Leave the later instruments and components for their phase, and
say in the checklist where they will be added.

Flag anything you edited that is **tracked by git** (`envs/${ENV}.env`, `envs/.env-mongo`) so
filled-in passwords are not committed.

## Step 5 — write `DEPLOYMENT_CHECKLIST.md`

Repo root. Gitignored — never commit it.

**Phase first, machine second.** One `##` section per phase, ending in its checkpoint; inside it,
one `###` group per machine, in the order the operator should work through them, each headed by the
machine's role. A step that genuinely applies everywhere goes in a group named `all machines`
(`both machines` for two). Never make the reader scan a mixed list to find out where they are
supposed to be sitting.

```markdown
# AlphaKraken deployment checklist
Generated <date> · ENV=<env> · <one-line summary of the setup>
Plan: walking skeleton on <instrument>, then <what phase 4-6 add>. Stops at phase <n>.

## Your setup
<the interview answers, one line each — makes the file self-contained>
<the machine names and what each one runs>

## Config already edited
<one item per file, what changed, and what the operator still has to fill in>

## Phase 0 — prerequisites and wiring
### all machines
### <db-host>
...
> **Checkpoint 0:** <the observable proof>

## Phase 1 — skeleton up, file discovery on <instrument>
### <db-host> — central components
### <worker-host> — mounts
### <infra-host> — airflow infrastructure
### <worker-host> — workers
### any machine — Airflow UI
> **Checkpoint 1:** the `instrument_watcher.<instrument>` log lists real files and they appear in
> the webapp. Nothing has been written to the instrument.

## Phase 2 — backup
> **Checkpoint 2:** …
## Phase 3 — first cluster job (MSQC)
> **Checkpoint 3:** …
## Phase 4 — real analysis software, remaining instruments
## Phase 5 — file mover, then remover  (not yet — see above)
## Phase 6 — optional components
```

Item shape — action, then the proof, nothing more:

```markdown
- [ ] `sudo mount -a` — [docs](docs/deployment.md#set-up-pool-bind-mounts)
  - Check: `ls <MOUNTS_PATH>/backup` lists the pool's content, `findmnt <MOUNTS_PATH>/backup` shows a `cifs` entry
```

Order strictly by dependency: `envs/${ENV}.env` filled before `mount.sh` (it sources the file to
read `MOUNTS_PATH` and aborts on unresolved placeholders); mount targets exist before `mount.sh`;
mounts before the containers that bind them; `bootstrap_airflow.sh --init` before any airflow
container; webserver up before connections are created in the UI; workers before infrastructure on
restarts.

### Standard checks

Use these rather than inventing weaker ones:

| After | Check |
|---|---|
| creating a mount target | `ls -ld <target>` exists, owned by the deployment account |
| `mount.sh … fstab` + `sudo mount -a` | `ls <target>` shows the **remote** content (not empty), `findmnt <target>` shows `cifs`. (`mount.sh <entity> mount` prints that listing itself.) |
| an instrument mount | the acquisition folder and its `Backup` folder are listed |
| filling `envs/${ENV}.env` on a non-db machine | from that machine, each db port answers: `nc -z <db-host> 27017`, then `5432`, then `6379` |
| `compose.sh … up` | `./compose.sh ps` — every service `healthy`/`running`, none restarting; `./compose.sh logs <svc>` free of errors |
| `dbs` profile | mongo logs show the three users created (`#mongodb-user-management`) |
| `bootstrap_airflow.sh --init` | Airflow UI → Admin → Pools and → Variables list the entries |
| `infrastructure` profile | Airflow UI, webapp and `:8090/docs` answer; no DAG import errors |
| `workers` profile | every expected worker shows up as an active Celery worker in the Airflow UI |
| the SSH connection | "Test" in the Airflow UI succeeds |
| unpausing `instrument_watcher.<id>` | its task log lists the real file names from the instrument; rows appear in the webapp |
| unpausing `acquisition_handler.<id>` | the file lands in the backup location, status reaches `done`, checksum matches |
| the first MSQC job | the slurm log in the runner's `slurm` view exists and exit code is 0; `msqc__*` columns are filled in the webapp |
| unpausing `file_mover.<id>` (phase 5) | a file moves to the instrument's `Backup` folder, and only then |
| unpausing `file_remover.<id>` (phase 5) | free space on the instrument grows as expected |
| nginx | the URLs answer over TLS and basic auth prompts; `./compose.sh logs nginx` clean |
| alerting | provoke one alert (e.g. stop `mongodb-service` briefly) and see the webhook fire |

## Step 6 — hand over

Point at `docs/maintenance.md#troubleshooting` for failures. Then tell the operator, in two lines,
what you edited, where the checklist is, and which checkpoint to stop at before continuing.
