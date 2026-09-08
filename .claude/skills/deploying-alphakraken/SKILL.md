---
name: deploying-alphakraken
description: Plans a first sandbox or production AlphaKraken deployment on custom infrastructure. Interviews the operator about available machines, compute, storage and instruments, edits the config files accordingly, and writes a tailored DEPLOYMENT_CHECKLIST.md grouped per machine, with the exact commands to run and a success check after each one. Use when setting up AlphaKraken on new hardware ("how do I deploy this on our machines", "set up AlphaKraken here", "deploy to our cluster"). Not for local development (see docs/development.md) and not for day-2 ops (see docs/instruments.md, docs/maintenance.md).
---

# Deploying AlphaKraken on custom infrastructure

`docs/deployment.md` is the source of truth. This skill does not repeat it — it decides **which**
of its steps apply to *this* operator's infrastructure, in **which order**, on **which machine**,
and **how to tell each step worked**.

Scope: `sandbox` and `production` only. A `local` setup is development, not deployment — point at
`docs/development.md#local-testing` and stop.

## Rules

1. **Link, never restate.** A checklist item is one imperative line + the machine + the command or
   file + a link `docs/deployment.md#anchor`. If you find yourself explaining *how* a documented
   step works, delete the explanation and link instead.
2. **Every action gets a check.** No item is done because a command exited 0. Give the observable
   proof as an indented `Check:` line — see *Standard checks* below.
3. **Commands are quoted verbatim** in the checklist (`./compose.sh …`, `./mount.sh …`,
   `docker build …`), so the operator can copy-paste. Substitute their real `ENV`, hostnames,
   instrument ids and profiles — no placeholders they have to resolve themselves.
4. **You edit config; the operator runs commands.** You may edit `envs/${ENV}.env`,
   `envs/alphakraken.${ENV}.yaml`, `envs/.env-airflow`, `envs/.env-mongo`, `docker-compose.yaml`,
   `misc/nginx.conf`. You never run `compose.sh`, `mount.sh`, `docker`, `sudo`, or touch
   `/etc/fstab`.
5. **Passwords by environment.** `sandbox` holds no valuable data: set simple, obvious passwords
   yourself (e.g. `sandbox`) so the operator is not blocked, and say so. `production`: never invent
   one — write `SET_STRONG_PASSWORD_NO_SPECIAL_CHARS` and make filling it a checklist item.
   Constraints in `#additional-steps-required-for-initial-sandboxproduction-deployment`.
6. If an answer maps to no documented path, say so plainly instead of improvising a deployment.

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
- Number of instruments to onboard now, and their vendors (thermo / bruker / sciex).

**Batch B — compute and storage**
- Where quanting jobs run: Slurm cluster over SSH / containers on the worker host (`docker` engine)
  / nothing yet (discovery only).
- Shared file system: CIFS shares reachable from the worker host / already mounted natively /
  none. Also: is it reachable from the compute nodes under a *different* path? (the "cluster view").
- Raw file backup target: pool folder (`local`) or S3.
- Confirm the read-only start (see below). Ask for a reason to deviate, not for a preference.

**Batch C — accounts and optional components** (multi-select where sensible)
- Two service accounts available (`kraken-write` with write access to backup, `kraken-read`
  read-only access to backup, cf. `#required-users`)? If only one exists, flag the least-privilege loss.
- Optional: nginx reverse proxy with TLS + basic auth · Slack/Teams alerting · nightly MongoDB
  backups · S3 upload worker · MCP server / REST API (both come with the `infrastructure` profile).

## Step 3 — decide

Map answers to steps. Always in the checklist:

| Step | Machine | Doc anchor |
|---|---|---|
| Install Docker + `python3`, clone repo at the same commit | every | `#setting-up-new-alphakraken-instance-workers-andor-infrastructure` |
| `echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow` — per machine, never copied | every | same |
| `export ENV=<env>` in every shell that runs `compose.sh`/`mount.sh` | every | `#deployment` |
| `./misc/bootstrap_airflow.sh --init` (once, ever — db init + Pools + Variables) | db host | `#one-time-initialization-of-airflow-infrastructure` |
| Review the bootstrapped Pools and Variables; size `cluster_slots_pool` to the real capacity | UI | `#setup-required-pools` |
| Airflow connection `cluster_ssh_connection` (real or dummy) | UI | `#setup-ssh-connection` |

Conditional:

| Answer | Adds |
|---|---|
| one machine | `dbs`, then `infrastructure`, then `workers` profiles on that host; `*_HOST` may stay at the compose service names |
| ≥2 machines | per-host env wiring (see below); bring-up in dependency order (dbs → infrastructure → workers); NTP time sync (`#additional-steps-required-for-initial-sandboxproduction-deployment`) |
| custom env name | copy `envs/sandbox.env` and `envs/alphakraken.sandbox.yaml` to the new name |
| CIFS shares | `sudo apt install cifs-utils`; `MOUNTS_PATH` **absolute**; create the mount targets first (`mount.sh` will not mount into a missing folder); one `./mount.sh <entity> fstab` per entity, pasted into `/etc/fstab` with passwords (`#set-up-pool-bind-mounts`). `airflow_logs` on every machine that runs an airflow container; `backup`, `output` and every instrument on worker machines |
| debugging / first try | offer `./mount.sh <entity> mount` instead (`#alternative-non-persistent-mounts`) |
| Slurm | cluster dir + `submit_job.sh` with adapted `partition`/`nodelist`; `runners[].view` paths = *cluster view*; AlphaDIA env named `alphadia-<version>` (`#on-the-cluster`, `#setup-alphadia-on-the-cluster`) |
| `docker` engine | `INSTALL_DOCKER_ENGINE=true` + `DOCKER_GID`; `docker build -t alphakraken-msqc msqc-extractor`; dummy ssh connection + Airflow var `debug_no_cluster_ssh=true`; size `cluster_slots_pool` to the host; settings entry in the webapp (`#standalone-deployment-without-a-cluster`) |
| no quanting yet | `skip_quanting: true` per instrument in the yaml |
| S3 backup | `backup.backup_type: s3`, `aws_default` connection, `s3_upload_pool`, `./compose.sh --profile s3 up --build -d` (`#s3-configuration-optional`) |
| each instrument | yaml block + worker service in `docker-compose.yaml` + mount + unpause DAGs (`docs/instruments.md`) |
| nginx | IPs in `misc/nginx.conf`, `htpasswd` file, cert or `nginx_no_ssl.conf`, `./compose.sh up nginx --build --force-recreate -d` (`#url-redirect`) |
| alerting | `general.notifications.*` in the yaml (`#monitoring--alerting`) |
| db backups | `misc/backup_db.sh` path + cron (`#automated-mongodb-database-backups`) |

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

### Read-only start (default)

A new instance does not write to instruments on day one. Propose this as the plan and only deviate
if the operator gives a reason:

1. **Discovery + metrics only.** Per instrument in `envs/alphakraken.${ENV}.yaml`:
   `file_move_delay_m: -1` (no file moving) and `min_free_space_gb: -1` (no file removing) —
   semantics in the yaml comments. Keep the `file_mover.*` and `file_remover.*` DAGs paused.
2. Then enable copying to backup, and watch it for a few days.
3. Then the file mover, then the remover — one at a time, checking the Airflow UI in between.

The checklist must carry the *later* steps too, as an explicit "not yet" item, so nobody assumes
purging works.

### Push back when

- **`root` as the OS account** owning the checkout or running `compose.sh`. `AIRFLOW_UID` would be
  `0`, so every container writes root-owned files into the mounts and logs. Insist on an
  unprivileged account (the operator's own, or a dedicated `kraken` login) with docker access.
  Same for the CIFS/pool accounts: use the dedicated `kraken-read`/`kraken-write` service accounts,
  not a root or admin account (`#required-users`).
- Slurm chosen but no shared file system reachable from both sides — the cluster view cannot be
  resolved; this is not a supported deployment.
- `docker` engine on a multi-machine production setup — mounting the docker socket into workers is
  root on the host (`#standalone-deployment-without-a-cluster`).
- Relative `MOUNTS_PATH` — breaks fstab lines and the docker job engine.
- One shared account instead of `kraken-read`/`kraken-write` — the backup pool loses its read-only
  guard.

## Step 4 — edit config

Make the edits from Rule 4, one file at a time, and list each in the checklist under
*Config already edited* so the operator reviews rather than repeats them. Everything that must be
edited is enumerated in `#summary` — check your edits against that list before moving on.

Flag anything you edited that is **tracked by git** (`envs/${ENV}.env`, `envs/.env-mongo`) so
filled-in passwords are not committed.

## Step 5 — write `DEPLOYMENT_CHECKLIST.md`

Repo root. Gitignored — never commit it.

**Group by machine, not by topic.** One `##` section per machine, in the order the operator should
work through them, each headed by the machine's role. A step that genuinely applies everywhere goes
in a section named `all machines` (`both machines` for two). Never make the reader scan a mixed list
to find out where they are supposed to be sitting.

```markdown
# AlphaKraken deployment checklist
Generated <date> · ENV=<env> · <one-line summary of the setup>

## Your setup
<the interview answers, one line each — makes the file self-contained>
<the machine names and what each one runs>

## Config already edited
<one item per file, what changed, and what the operator still has to fill in>

## 0. all machines — prerequisites
## 1. <db-host> — central components
## 2. <worker-host> — mounts
## 3. <infra-host> — airflow infrastructure
## 4. <worker-host> — workers
## 5. cluster / compute
## 6. any machine — Airflow UI, one-time
## 7. <instrument PC> + UI — per instrument
## 8. optional components (grouped by machine)
## 9. all machines — end-to-end verification
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
| adding an instrument | its `*.<instrument_id>` DAGs appear; after unpausing `instrument_watcher`, its log lists real files |
| the SSH connection | "Test" in the Airflow UI succeeds |
| nginx | the URLs answer over TLS and basic auth prompts; `./compose.sh logs nginx` clean |
| alerting | provoke one alert (e.g. stop `mongodb-service` briefly) and see the webhook fire |
| the whole pipeline | one small raw file: discovered → copied to backup → job submitted → metrics in the webapp |

## Step 6 — hand over

Point at `docs/maintenance.md#troubleshooting` for failures. Then tell the operator, in two lines,
what you edited and where the checklist is.
