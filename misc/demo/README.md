# Self-contained AlphaKraken demo

Runs the whole pipeline on a single Linux host, with no acquisition PC, no Slurm cluster and no
shared file system: a feeder script fakes an instrument acquiring one Thermo `.raw` file every 21
minutes, the `docker` job engine runs the [msqc-extractor](../../msqc-extractor) in a container, and
all four components are served behind one nginx under a single UUID path prefix.

The design rationale is in [design_docs/DEMO_SETUP_PLAN.md](../../design_docs/DEMO_SETUP_PLAN.md).

## Prerequisites

- A Linux host with docker, at least 16 GB RAM and 4 CPU cores.
- A TLS certificate in `certs/` (`fullchain.pem` + `privkey.pem`) valid for the demo host.
- `envs/.env-airflow`, created once with `echo "AIRFLOW_UID=$(id -u)" > envs/.env-airflow`.
- `pip install alphabase progressbar2` on the host, for the raw file download only.
- No other AlphaKraken environment running: the demo reuses the fixed postgres, redis and MongoDB
  host ports, so `ENV=local` must be stopped first.

## Starting the demo

```bash
python misc/demo/download_raw_files.py     # ~3 x 1 GB from MPIB datashare, once
DEMO_HOST=my-host.example.org misc/demo/setup_demo.sh
```

`setup_demo.sh` renders the configuration, builds the images, starts the stack, configures Airflow
(pools, `debug_no_cluster_ssh`, unpausing the demo DAGs), seeds the project and settings, and prints
the URLs. It is safe to re-run — the demo UUID is generated once and kept.

Then start the feeder and install the pruner:

```bash
nohup misc/demo/feed_instrument.sh > misc/demo/.state/feeder.log 2>&1 &
crontab -e   # 0 * * * * <repo>/misc/demo/prune_demo_data.sh >> <repo>/misc/demo/.state/prune.log 2>&1
```

`DEMO_HOST` defaults to `hostname -f`. It has to be right, because Airflow generates absolute URLs
from it; re-run `setup_demo.sh` if the host name changes.

## URLs

With `UUID=$(cat misc/demo/.state/uuid)`:

| component | URL |
| --- | --- |
| webapp | `https://$DEMO_HOST/$UUID/` |
| Airflow UI | `https://$DEMO_HOST/$UUID/airflow/` (`airflow` / `airflow`) |
| MCP server | `https://$DEMO_HOST/$UUID/mcp` |
| REST API | `https://$DEMO_HOST/$UUID/api/docs` |

Anything outside the prefix gets a 404. There is no basic auth: the UUID is the only barrier, so
treat it as a share link, not as a secret — it shows up in nginx logs and browser history.

## What to expect

The feeder copies the next raw file into `mounts/instruments/demo1/` every 21 minutes, appending a
timestamp to the name. Per file:

1. `instrument_watcher.demo1` picks it up and starts `acquisition_handler.demo1`.
2. Monitoring finishes when the **next** file appears — an acquisition counts as done once exactly
   one new file shows up (cf. `AcquisitionMonitor`). The feeder interval therefore paces the whole
   pipeline, and this is why the feeder cycles forever instead of stopping.
3. The file is checksummed and copied to `mounts/backup/demo1/<year_month>/`.
4. `acquisition_processor.demo1` starts a `kraken-custom-<raw file>` container that writes
   `msqc_results.tsv`, `msqc_tic.tsv` and `log.txt` to
   `mounts/output/ADIAMA/out_<raw file>/custom/`.
5. Metrics appear in the webapp, the REST API and the MCP server.

The raw files carry `ADIAMA` as an underscore-separated token, which is what associates them with
the seeded project of that name.

To watch a first run without waiting 21 minutes, start the feeder with `INTERVAL_S=120`.

## Housekeeping

`prune_demo_data.sh` keeps only the newest `KEEP_LAST=2` raw files in the pool backup, the quanting
output and the instrument backup folder — roughly 10 GB on disk, regardless of how long the demo
runs. It is safe to run at any time: keeping the newest two covers whatever is in flight, so it
cannot race the pipeline. **MongoDB is not touched**, so the metrics history in the webapp keeps
growing while the disk usage stays flat.

The pool backup additionally keeps every file that the instrument backup folder still holds, so it
lags one prune cycle behind and can carry a few GB more than `KEEP_LAST` suggests. This keeps the
pool copy from vanishing while the instrument copy is still there, which is the order the remover
expects.

Without the cron job, `mounts/{backup,output}` grow by roughly 68 runs per day. Check with
`du -sh misc/demo/mounts/*`.

As a backstop, the feeder skips an acquisition when less than `REQUIRED_SPACE_PERCENT=220` of the
source file size is free on the instrument mount — twice the file size, for the raw file and its
pool backup copy, plus 10% headroom. It retries the same file at the next interval, so a full disk
pauses the demo instead of breaking it.

Airflow logs under `mounts/airflow_logs` are not pruned (see `misc/archive_airflow_logs.sh`).

## Stopping and resetting

```bash
pkill -f feed_instrument.sh
ENV=demo ./compose.sh --profile demo down
```

Full reset, which also drops the accumulated history:

```bash
pkill -f feed_instrument.sh
ENV=demo ./compose.sh --profile demo down -v
sudo rm -rf misc/demo/mounts mongodb_data_demo airflowdb_data_demo
misc/demo/setup_demo.sh
```

## Known limitations

- **Only three source files exist**, and cycling re-feeds the same bytes, so every metric repeats
  with a period of three: the QC trend plots are a flat sawtooth, not realistic instrument drift.
  Add more varied `.raw` files to `misc/demo/raw_files/` to fix that.
- **The settings show up as software type `custom`**, not `msqc`, because the `docker` job engine is
  only supported for `custom`. The *metrics* type is `msqc`, so the metrics are labelled correctly.
- **SCIEX `.wiff` files are not supported** by the msqc container: it reads Thermo files through the
  `coreclr` .NET runtime, which cannot handle SCIEX.

## Generated files

Rendered by `setup_demo.sh` from `templates/`, all gitignored — edit the templates, not these:

- `envs/demo.env`
- `envs/alphakraken.demo.yaml`
- `misc/demo/.state/nginx.conf`
- `misc/demo/.state/uuid` (generated once)
