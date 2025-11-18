
## Deployment
This guide is both valid for a local setup (without connection to pool or cluster), and for sandbox/production setups.

Upfront, set an environment variable `ENV`, which is either `local`, `sandbox`, or `production`, e.g.
```bash
ENV=local && export ENV=$ENV
```
This will use the environment variables defined in `envs/${ENV}.env` and point shell scripts to the correct configuration.

### Overview
<img src="deployment.jpg" alt="deployment overview" style="max-width: 600px;"/>

### Deployment workflow: 'local' vs. 'sandbox' vs. 'production'
All features should be tested on `local` before deploying them to the `sandbox` environment
for further testing. `sandbox` is technically equivalent to `production`, but it does not contain any valuable
data and therefore it's perfectly fine to break and/or wipe it.
There, depending on the scope of the feature, and of the likeliness of breaking something,
another test with real data might be necessary.

Use common sense when deciding the scope of testing:
- Cosmetic changes (e.g. webapp text/styling): test on local should suffice
- Infrastructure changes (e.g. new DB fields) or data handling changes (e.g. file operations): test on local and sandbox

Only a well-tested feature should be deployed to production. Make sure a pull request is always self-contained
and 'shippable', i.e. deployment to production should be possible at any time. After a production deployment,
it is good practise to check for errors in the Airflow UI.

### Initial deployment
All commands in this Readme assume you are in the root folder of the repository.
Please note that running and developing the alphakraken is only tested for MacOS and Linux
(the UI can be accessed from any OS, of course).

#### Setting up new AlphaKraken instance (workers and/or infrastructure)
The following steps are required for both the `local` and the `sandbox`/`production` deployments.
For the latter, additional steps are required, see [here](#additional-steps-required-for-initial-sandboxproduction-deployment).

1. Install [Docker](https://docs.docker.com/engine/install/ubuntu/) and `python3`.

2. Clone the repository into a folder and `cd` into it.

3. Set the current user as the user within the airflow containers and get the correct permissions on the "logs"
directory (otherwise, `root` would be used)
```bash
echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow
```

#### One-time initialization of Airflow infrastructure
This needs to be done only once for a brand-new installation. It is not required
e.g. to spin up another instance hosting workers only.

1. On the PC that will host the internal Airflow database (this is not the MongoDB!) run
```bash
./compose.sh --profile dbs up airflow-init
```

2. In the Airflow UI, set up the SSH connection to the cluster (see [below](#setup-ssh-connection)).
If you don't want to connect to the cluster, just create the connection of type
"ssh" and name "cluster_ssh_connection" with some dummy values for host, username, and password.
In this case, make sure to set the Airflow variable `debug_no_cluster_ssh=True` (see below).

3. In the Airflow UI, set up the required Pools (see [below](#setup-required-pools)).

#### Run the containers (local version)
Start all docker containers required for local testing with
```bash
./compose.sh --profile local up --build -d
```
After startup, the airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the Streamlit webapp on http://localhost:8501/ .

To spin all containers down again, use
```bash
./compose.sh --profile local down
```
A more graceful 'warm shutdown' can be achieved by
```bash
./compose.sh --profile local stop
```

See below for [some useful Docker commands](maintenance.md/#some-useful-docker-commands).


### Additional steps required for initial sandbox/production deployment

The main differences between the `local` and the `sandbox`/`production` deployments are:
- `local` has all services running on the same machine within the same docker-compose network,
whereas `sandbox`/`production` is per default distributed over two machines
- `sandbox`/`production` needs additional steps to configure the cluster and the network bind mounts

The different services can be distributed over several machines. The only important thing is that there
it exactly one instance of each of the 'central components': `postgres-service`, `redis-service`, and `mongodb-service`.
One reasonable setup is to have the central components on one machine,
and Airflow infrastructure (scheduler & webserver), workers and WebApp on another.
This is the current setup in the docker-compose, which is reflected by the
profiles `dbs`, and `infrastructure`/`workers`/`webapp`, respectively. If you move one of the central components
to another machine, you might need to adjust the `*_HOST` variables in the
`./env/${ENV}.env` files (see comments there). Of course, one machine could also host them all.

Make sure that the time is in sync between all machines, e.g. by using the same NTP time server.

For production: set strong passwords for `AIRFLOW_PASSWORD`, `MONGO_PASSWORD`, and `POSTGRES_PASSWORD`
in `./env/production.env` and `MONGO_INITDB_ROOT_PASSWORD` in `./env/.env-mongo`.
Make sure they don't contain special characters (e.g. '\', '#', '@', '$', ..) as they might interfere with name resolution in `docker-compose.yaml`.

#### Required users
Two different users are recommended for the deployment:
one ("`kraken-write`") that has write access to the `backup` pool folder,
and one  ("`kraken-read`") that has only _read_ access.
Both users should be able to write to the `logs` and `output` directories, and to read from `settings`.

#### On the PC (VM) hosting the dbs (MongoDB, Airflow Postgres, Redis)

1. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`export ENV=production`).

2. Run the MongoDB, airflow db & redis services
```bash
./compose.sh --profile dbs up --build -d
```

#### On the PC (VM) hosting the airflow infrastructure (scheduler, webserver)

1. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`export ENV=production`).

1. Set up the [pool bind mounts](#set-up-pool-bind-mounts) for `airflow_logs` only. Here, the logs of the individual task runs will be stored
for display in the Airflow UI.

2. Run the worker and/or infrastructure containers
```bash
./compose.sh --profile infrastructure up --build -d
```




#### On the PC (VM) hosting the workers
1. `ssh` into the PC/VM, `cd` to the alphakraken source directory, and set `export ENV=sandbox` (`export ENV=production`).

2. Set up the [pool bind mounts](#set-up-pool-bind-mounts) for all instruments and `logs`, `backup` and `output`.

3. Run the worker and/or infrastructure containers
```bash
./compose.sh --profile workers up --build -d
```


#### URL redirect
In case you want to set up a URL redirect from one PC to one or multiple others, do the following on the redirecting PC:
1. Edit `misc/nginx.conf`: substitute the placeholder IP adresses (e.g. `255.255.0.1`) with the correct ones.
2. Start the respective container `./compose.sh up nginx --build --force-recreate -d`, see the folder `nginx_logs` for logs

#### On the cluster
1. Log into the cluster using the `kraken-read` user.
2. Create a directory (to store the submit script and job logs), e.g.
```bash
mkdir /fs/pool-2/slurm
```
and set the `locations.slurm.absolute_path` key in `envs/alphakraken.${ENV}.yaml` to this value.

3. Copy the cluster run script `submit_job.sh` to `/fs/pool-2/slurm` and adapt the `partition` (and optionally `nodelist`) directives.
Make sure to update also this file when deploying a new version of the AlphaKraken.

4. Set up AlphaDIA (see [below](#setup-alphadia-on-the-cluster)).

### General note on how Kraken gets to know the data

Each worker needs two 'views' on the raw and output data.

The first view ("worker PC view") enables read/write access,
by mounting on the Kraken host PC a specific (network) folder (e.g. `\\pool-backup\pool-backup` or `\\pool-output\pool-output`)
using `cifs` mounts (wrapped by `mount.sh`)
to a target folder and then mapping this target folder to a worker container
in `docker-compose.yaml`, such that it can be accessed in a unified manner from within the containers (cf. `InternalPaths`).

The second view ("cluster view") is the location of the data on the shared filesystem as seen from the Slurm cluster
(e.g. `/fs/pool/pool-backup` or `/fs/pool/pool-output`),
which is required to set the paths for the cluster jobs correctly.

For instruments, only the first type of view is required, as the cluster does not access the instruments directly.

All paths are configured in the `locations` section of the `envs/alphakraken.${ENV}.yaml` file (see comments in `alphakraken.local.yaml`
for details).

### Set up pool bind mounts
All airflow components (webserver, scheduler and workers) need a bind mount to a pool folder to read and write `airflow_logs`.
The workers need in addition bind mounts set up to the pool filesystems for `backup` and reading AlphaDIA `output` data,
and to the instrument PCs.

This section describes the setup of the bind mounts via `/etc/fstab`, see [below](#alternative-non-persistent-mounts)
for an alternative.

IMPORTANT NOTE: it is absolutely crucial that the mounts are set correctly (as provided by the `envs/alphakraken.${ENV}.yaml` file)
as the workers operate only on docker-internal paths and cannot verify the correctness of the mounts.

0. (on demand) Install the `cifs-utils` package (otherwise you might get errors like
`CIFS: VFS: cifs_mount failed w/return code = -13` or `mount(2)  system call failed: No route to host.`)
```bash
sudo apt install cifs-utils
```

1. Create folders `settings`, `output`, and `airflow_logs` in the desired pool location(s), e.g. under `/fs/pool/pool-alphakraken`.

2. Make sure the variables `MOUNTS_PATH` in the `envs/${ENV}.env` file and `locations.general.mounts_path`
in the `envs/alphakraken.${ENV}.yaml` file are set correctly.

3. Create `fstab` entries for the backup, output, and logs folders, and all  instruments (here: `test1`):
```bash
./mount.sh backup fstab
./mount.sh output fstab
./mount.sh logs fstab
./mount.sh test1 fstab
```

4. Add the created entries to the `/etc/fstab` file and set the correct password for each entry.


Note: for now, user `kraken-write` should only have read access to the backup pool folder, but needs `read/write` on the `output`
folder.


#### Alternative: non-persistent mounts
You can also mount the folder manually, at the disadvantage that it needs to be re-done after a reboot. However,
this can be handy for debugging on initial setup.

1. Disable the automatic restart of the Docker service
([cf. here](https://docs.docker.com/engine/install/linux-postinstall/#configure-docker-to-start-on-boot-with-systemd))
```bash
sudo systemctl disable docker.service
sudo systemctl disable docker.socket
sudo systemctl disable containerd.service
```
This is required, as an automated restart without mounts would leave the system in an inconsitent state.

2. Set up all mounts for all instruments (`test1`, ..) and the other folders (you will be asked for passwords):
```bash
for entity in test1 backup output logs; do
  ./mount.sh $entity mount
done
```

If you need to remount one of the folders, pass the `umount` flag, e.g.
`./mount.sh output umount`.

3. Start the docker service
```bash
sudo systemctl start docker
```


### Setup SSH connection
At least one connection is required to interact with the Slurm cluster.

1. Open the Airflow UI, navigate to "Admin" -> "Connections" and click the "+" button.
2. Fill in the following fields:
    - Connection Id: `cluster_ssh_connection`
    - Conn Type: `SSH`
    - Host: `<cluster_head_node_ip>`  # the IP address of a cluster head node, in this case `<cluster_head_node>`
    - Username: `<user name of user kraken-read>`
    - Password: `<password of user kraken-read>`
3. (optional) Click "Test" to verify the connection.
4. Click "Save".
Note: make sure to use the `kraken-read` user with read-only access to the backup pool folder.

You can define multiple connections (name needs to start with `cluster_ssh_connection`) to increase robustness, e.g. in case one head node is down.

### Setup required pools
Pools are used to limit the number of parallel tasks for certain operations. They are managed via the Airflow UI
and need to be created manually once.
1. Open the Airflow UI, navigate to "Admin" -> "Pools".
2. For each pool defined in `settings.py:Pools`, create a new pool with a sensible value (see suggestions in the `Pools` class).

### Setup AlphaDIA on the cluster
For details on how to install AlphaDIA on the Slurm cluster, follow the AlphaDIA
[https://github.com/MannLabs/alphadia/blob/main/docs/installation.md#slurm-cluster-installation](Readme).

In a nutshell, to install a certain version, e.g. `VERSION=1.7.0`:

1. Log in (make sure to use the same user as configured in the [SSH connection](#setup-ssh-connection)!)
```bash
ssh kraken-read@<cluster_head_node>
```
2. create a new conda environment and activate it
```bash
conda create --name alphadia-${VERSION} python=3.11 -y && conda activate alphadia-${VERSION}
```
3. Install desired version
```bash
pip install "alphadia[stable]==${VERSION}"
```
Make sure the environment is named `alphadia-${VERSION}`, as this is the scheme that is expected by the module starting
the AlphaDIA jobs.
Also, don't forget to install `mono` (cf. AlphaDIA Readme).

### Summary
The following files need to be edited to customize your deployment:
- `envs/.env-airflow`: set the current user as the user within the airflow containers
- `envs/.env-mongo`: set the MongoDB root password
- `envs/${ENV}.env`: set the environment variables for the basic wiring of components
- `envs/alphakraken.${ENV}.yaml`: set up the paths and add a configuration for each instrument
- `docker-compose.yaml`: add a worker for each instrument
- `airflow_src/plugins/cluster_scripts/submit_job.sh` (cluster-local copy): configure partition and nodelist

### Deploying new code versions
These steps need to be done on all machines that run alphakraken services.
Make sure the code is always consistent across all machines!
0. If in doubt that something could break, create a backup copy of the `mongodb_data_${ENV}` and `airflowdb_data_${ENV}` folders (on the machine that hosts the DBs).
1. On each machine, pull the most recent version of the code from the repository using `git pull`.
2. Check if there are any special changes to be done (e.g. updating `submit_job.sh` on the cluster,
new mounts, new environment variables, manual database interventions, ..) and apply them.
3. (when deploying workers) To avoid copying processes being interrupted, in the Airflow UI set the size of the `file_copy_pool` to 0 and wait until all `copy_raw_file` tasks are finished.
4. Stop all docker compose services that need to be updated across all machines using the `./compose.sh --profile $PROFILE stop` command, once with `$PROFILE` set to `workers`,
and once to `infrastructure`.
5. Restart all docker compose services again, first the `workers` services, then the `infrastructure` services (with the `--build` flag).
6. Set the size of `file_copy_pool` to the number it was before.
7. Normal operation should be resumed after about 5 minutes. Depending on when they were shut down, some tasks
could be in an `error` state though. Check after a few hours if some files are stuck and resolve the issues with the Airflow UI.

## Monitoring & alerting
There is a basic monitoring system in place that sends messages to a Slack channel in the following cases
- MongoDB is not reachable
- last heartbeat of a worker is older than 15 minutes
- a pile up of non-terminal status (e.g. "quanting") is detected
- the free disk space of an instrument is below 200 GB

This component allows to detect issues early and to react before they become critical.
See the `monitoring` folder for details.

## MongoDB User Management
AlphaKraken implements a role-based access control system for MongoDB with three user types:

1. `MONGO_USER_READWRITE`: Full read-write access to the database, used by Airflow workers that need to update file status
2. `MONGO_USER_READ`: Read-only access to all collections, used by monitoring service
3. `MONGO_USER_WEBAPP`: Custom role with read access to all collections, but write access only to project and settings collections

This separation follows the principle of least privilege, ensuring each component has only the permissions it needs.


## MCP Servers
Set up the MCP server using the following configuration:
```
{
  "mcpServers": {
    "AlphaKraken": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "--network", "host",
        "-e", "MONGO_PORT=<MONGO_PORT>",
        "-e", "MONGO_HOST=<MONGO_HOST>",
        "-e", "MONGO_USER=<MONGO_USER_READ>",
        "-e", "MONGO_PASSWORD=<MONGO_PASSWORD_READ>",
        "-e", "MCP_TRANSPORT=stdio",
        "mannlabs/alphakraken-mcp-server:latest"
      ]
    }
  }
}
```
where the variables need to be set according to the `envs/${ENV}.env` file.
`MCP_TRANSPORT` can also be set to `streamable-http` to run the server in http mode (default port: 8089, override by `MCP_PORT`).

Alternatively, create the image yourself. First build the `alphakraken-mcp-server` container
```
docker build -t alphakraken-mcp-server -f mcp-server/Dockerfile .
```
and check if it start without errors
```bash
docker run -t alphakraken-mcp-server
```
Finally, substitute `mannlabs/alphakraken-mcp-server:latest` -> `alphakraken-mcp-server` in the above configuration.


## Automated MongoDB Database Backups

Optionally, you can set up automated backups of the MongoDB database using the provided backup script:

1. **Configure the backup mount**: Add an additional mount for backup storage to your docker-compose setup. The script expects backup storage at `/home/kraken-user/alphakraken/production/mounts/db_backups`.

2. **Configure the backup script**: Edit `misc/backup_db.sh` and set the correct path for `MONGODB_FOLDER` to point to your MongoDB data directory (typically `mongodb_data_production` or similar).

3. **Set up automated backups** using cron:
   ```bash
   # Add to crontab (sudo crontab -e)
   0 0 * * * /path/to/alphakraken/misc/backup_db.sh nightly    # Daily backup
   0 * * * * /path/to/alphakraken/misc/backup_db.sh hourly     # Hourly backup
   ```

The script creates rotating daily backups (named by weekday) and maintains an hourly backup. All backup operations are logged to the backup directory.

### Restore a backup
To restore a backup, stop the MongoDB service and replace the contents of the MongoDB data directory with the contents of the desired backup folder. Then restart the MongoDB service.

## S3 Upload Configuration (Optional)
AlphaKraken supports uploading raw files to S3-compatible object storage as an alternative to local backup. When enabled, files are uploaded in parallel to the local copy process.

### Configuration

1. **Enable S3 in YAML config** (`envs/alphakraken.${ENV}.yaml`):
```yaml
backup:
  backup_type: s3  # Change from 'local' to 's3'
  s3:
    region: eu-central-1
    bucket_prefix: alphakraken
```

2. **Configure AWS connection in Airflow UI**:
   - Navigate to "Admin" -> "Connections" -> "+" button
   - Connection Id: `aws_default`
   - Connection Type: `Amazon Web Services`
   - AWS Access Key ID: `<your_access_key>`
   - AWS Secret Access Key: `<your_secret_key>`
   - Extra: `{"region_name": "eu-central-1"}` (optional, overrides YAML config)
   - Click "Save"

   For S3-compatible services (e.g., custom endpoints):
   - Extra: `{"endpoint_url": "https://your-s3-endpoint.com"}`

3. **Create S3 upload pool in Airflow UI**:
   - Navigate to "Admin" -> "Pools" -> "+" button
   - Pool Name: `s3_upload_pool`
   - Slots: `2` (max concurrent S3 uploads)
   - Click "Save"

4. **Deploy S3 uploader worker**:
   The S3 uploader runs as a dedicated Celery worker. Start it using:
   ```bash
   ./compose.sh --profile workers up airflow-worker-s3-uploader --build -d
   ```

### How it works

- Files are organized in S3 by project and instrument: `{bucket_prefix}-{project_id}/{instrument_id}/{file_path}`
- Upload happens in 500MB chunks with automatic multipart upload
- Upload integrity is verified using ETag comparison
- Upload status is tracked in the database (`backup_status` field: `UPLOAD_IN_PROGRESS`, `UPLOAD_DONE`, `UPLOAD_FAILED`)
- Failed uploads do not block file processing and can be retried manually

### Monitoring

Check S3 upload status:
- **Airflow UI**: Monitor the `s3_uploader` DAG runs
- **Database**: Query the `s3_upload_path` and `backup_status` fields in the `file_raw` collection
- **Logs**: Check worker logs for upload progress and errors
