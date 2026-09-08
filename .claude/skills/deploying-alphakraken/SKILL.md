---
name: deploying-alphakraken
description: Plans the first AlphaKraken deployment on custom infrastructure. Interviews the operator about available machines, compute, storage and instruments, edits the config files accordingly, and writes a tailored DEPLOYMENT_CHECKLIST.md with the exact commands to run on each machine. Use when setting up AlphaKraken on new hardware ("how do I deploy this on our machines", "set up AlphaKraken here", "deploy to our cluster"). Not for day-2 ops (adding instruments, upgrading) — see docs/instruments.md and docs/maintenance.md.
---

# Deploying AlphaKraken on custom infrastructure

`docs/deployment.md` is the source of truth. This skill does not repeat it — it decides **which**
of its steps apply to *this* operator's infrastructure, in **which order**, and on **which machine**.

## Rules

1. **Link, never restate.** A checklist item is one imperative line + the machine + the command or
   file + a link `docs/deployment.md#anchor`. If you find yourself explaining *how* a documented
   step works, delete the explanation and link instead.
2. **Commands are quoted verbatim** in the checklist (`./compose.sh …`, `./mount.sh …`,
   `docker build …`), so the operator can copy-paste. Substitute their real `ENV`, instrument ids
   and profiles — no placeholders that they have to resolve themselves.
3. **You edit config; the operator runs commands.** You may edit `envs/${ENV}.env`,
   `envs/alphakraken.${ENV}.yaml`, `envs/.env-airflow`, `docker-compose.yaml`, `misc/nginx.conf`.
   You never run `compose.sh`, `mount.sh`, `docker`, `sudo`, or touch `/etc/fstab`.
4. **Passwords stay with the operator.** Never invent or write real passwords into env files —
   leave a clearly marked placeholder and a checklist item.
5. If an answer maps to no documented path, say so plainly instead of improvising a deployment.

## Step 1 — read

Read `docs/deployment.md` in full, and the comments in `envs/alphakraken.local.yaml`. Skim
`README.md#system-requirements`. Do not start the interview before that.

## Step 2 — interview

Three `AskUserQuestion` batches, in this order. Skip any question already answered in the prompt.

**Batch A — topology**
- Target environment: `local` (test, no cluster/pool) / `sandbox` / `production` / other name.
- Machine layout: one machine for everything / two (dbs + rest) / three or more
  (dbs, infrastructure, workers) / already-running instance, adding a machine.
- Host OS and whether Docker + `python3` are already installed.
- Number of instruments to onboard now, and their vendors (thermo / bruker / sciex).

**Batch B — compute and storage**
- Where quanting jobs run: Slurm cluster over SSH / containers on the worker host (`docker` engine)
  / nothing yet (discovery + copying only).
- Shared file system: CIFS shares reachable from the worker host / already mounted natively /
  none. Also: is it reachable from the compute nodes under a *different* path? (the "cluster view").
- Raw file backup target: pool folder (`local`) or S3.
- Rollout ambition: full pipeline immediately, or read-only first (no copying/moving/purging on the
  instruments). Recommend read-only first for a production instrument — see *Gradual rollout* below.

**Batch C — accounts and optional components** (multi-select where sensible)
- Two service accounts available (`kraken-write` with write access to backup, `kraken-read`
  read-only, cf. `#required-users`)? If only one exists, flag the least-privilege loss.
- Optional: nginx reverse proxy with TLS + basic auth · Slack/Teams alerting · nightly MongoDB
  backups · S3 upload worker · MCP server / REST API (both come with the `infrastructure` profile).

## Step 3 — decide

Map answers to steps. Always in the checklist:

| Step | Machine | Doc anchor |
|---|---|---|
| Install Docker + `python3`, clone repo | every | `#setting-up-new-alphakraken-instance-workers-andor-infrastructure` |
| `echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow` | every | same |
| `export ENV=<env>` in every shell that runs `compose.sh`/`mount.sh` | every | `#deployment` |
| `./misc/bootstrap_airflow.sh --init` (once, ever — db init + Pools + Variables) | db host | `#one-time-initialization-of-airflow-infrastructure` |
| Review the bootstrapped Pools and Variables; size `cluster_slots_pool` to the real capacity | UI | `#setup-required-pools` |
| Airflow connection `cluster_ssh_connection` (real or dummy) | UI | `#setup-ssh-connection` |

Conditional:

| Answer | Adds |
|---|---|
| one machine, `local` | `./compose.sh --profile local up --build -d` — that is the whole bring-up |
| one machine, real env | `dbs`, then `infrastructure`, then `workers` profiles on the same host |
| ≥2 machines | adjust `*_HOST` in `envs/${ENV}.env`; per-machine profile bring-up in dependency order (dbs → infrastructure → workers); NTP time sync (`#additional-steps-required-for-initial-sandboxproduction-deployment`) |
| custom env name | copy `envs/sandbox.env` and `envs/alphakraken.sandbox.yaml` to the new name |
| any real env | strong passwords, no special characters, in `envs/${ENV}.env` + `envs/.env-mongo` (same anchor) |
| CIFS shares | `sudo apt install cifs-utils`; `MOUNTS_PATH` **absolute**; one `./mount.sh <entity> fstab` per entity, entries pasted into `/etc/fstab` with passwords (`#set-up-pool-bind-mounts`). `airflow_logs` on every airflow machine; `backup`, `output` and every instrument on worker machines |
| debugging / first try | offer `./mount.sh <entity> mount` instead (`#alternative-non-persistent-mounts`) |
| Slurm | cluster dir + `submit_job.sh` with adapted `partition`/`nodelist`; `runners[].view` paths = *cluster view*; AlphaDIA env named `alphadia-<version>` (`#on-the-cluster`, `#setup-alphadia-on-the-cluster`) |
| `docker` engine | `INSTALL_DOCKER_ENGINE=true` + `DOCKER_GID`; `docker build -t alphakraken-msqc msqc-extractor`; dummy ssh connection + Airflow var `debug_no_cluster_ssh=true`; size `cluster_slots_pool` to the host; settings entry in the webapp (`#standalone-deployment-without-a-cluster`) |
| no quanting yet | `skip_quanting: true` per instrument in the yaml |
| S3 backup | `backup.backup_type: s3`, `aws_default` connection, `s3_upload_pool`, `./compose.sh --profile s3 up --build -d` (`#s3-configuration-optional`) |
| each instrument | yaml block + worker service in `docker-compose.yaml` + mount + unpause DAGs (`docs/instruments.md`) |
| nginx | IPs in `misc/nginx.conf`, `htpasswd` file, cert or `nginx_no_ssl.conf`, `./compose.sh up nginx --build --force-recreate -d` (`#url-redirect`) |
| alerting | `general.notifications.*` in the yaml (`#monitoring--alerting`) |
| db backups | `misc/backup_db.sh` path + cron (`#automated-mongodb-database-backups`) |

### Gradual rollout

A first production instance should not write to instruments on day one. The per-instrument keys in
`envs/alphakraken.${ENV}.yaml` gate this (`skip_processing`, `skip_quanting`, `file_move_delay_m`,
`min_free_space_gb` — semantics in the yaml comments). Propose: discovery + metrics only, then
enable copying, then moving and purging, checking the Airflow UI between each.

### Push back when

- Slurm chosen but no shared file system reachable from both sides — the cluster view cannot be
  resolved; this is not a supported deployment.
- `docker` engine on a multi-machine production setup — mounting the docker socket into workers is
  root on the host (`#standalone-deployment-without-a-cluster`).
- Relative `MOUNTS_PATH` — breaks fstab lines and the docker job engine.
- One shared account instead of `kraken-read`/`kraken-write` — the backup pool loses its read-only
  guard.

## Step 4 — edit config

Make the edits from Rule 3, one file at a time, and list each in the checklist under
*Config already edited* so the operator reviews rather than repeats them. Everything that must be
edited is enumerated in `#summary` — check your edits against that list before moving on.

## Step 5 — write `DEPLOYMENT_CHECKLIST.md`

Repo root. Gitignored — never commit it. Structure:

```markdown
# AlphaKraken deployment checklist
Generated <date> · ENV=<env> · <one-line summary of the setup>

## Your setup
<the interview answers, one line each — makes the file self-contained>

## Config already edited
- [ ] Review `envs/<env>.env` — <what changed>
...

## 0. Prerequisites (every machine)
- [ ] **<host>** Install Docker and python3 — [docs](docs/deployment.md#setting-up-new-alphakraken-instance-workers-andor-infrastructure)
...
## 1. Central components (<host>)
## 2. Mounts (<host>)
## 3. Infrastructure (<host>)
## 4. Workers (<host>)
## 5. Compute
## 6. Airflow UI, one-time
## 7. Instruments
## 8. Optional components
## 9. Verify
```

Item shape — machine, action, command/file, link, nothing more:

```markdown
- [ ] **db-vm** `./compose.sh --profile dbs up --build -d` — [docs](docs/deployment.md#on-the-pc-vm-hosting-the-dbs-mongodb-airflow-postgres-redis)
- [ ] **worker-pc** Paste the generated fstab lines into `/etc/fstab`, add passwords — [docs](docs/deployment.md#set-up-pool-bind-mounts)
```

Order strictly by dependency: mounts exist before the containers that bind them;
`bootstrap_airflow.sh --init` before any airflow container; webserver up before connections are
created in the UI; workers before infrastructure on restarts.

## Step 6 — verify

Close the checklist with checks, not prose: Airflow UI reachable and DAGs green, webapp reachable,
a test file picked up end to end, `./compose.sh logs <service>` clean. Point to
`docs/maintenance.md#troubleshooting` for failures. Then tell the operator, in two lines, what you
edited and where the checklist is.
