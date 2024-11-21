# AlphaKraken

A fully automated data processing and analysis system for mass spectrometry experiments:
- monitors acquisitions on mass spectrometers
- copies raw data to a backup location
- runs AlphaDIA on every sample and provides metrics in a web application

## User quick-start

Important note: to not interfere with the automated processing, please stick to the following simple rule:

> Do not touch the acquisition folders*!

*i.e. the folders where the acquisition software writes the raw files to. In particular,
do not **delete**, **rename** or **open** files in these folders and do not **copy** or **move** files *from* or *to* these folders!

Regular users should find all required documentation in the [AlphaKraken WebApp](http://<kraken_url>).
The rest of this Readme is relevant only for developers and administrators.


## Deployment
This guide is both valid for a local setup (without connection to pool or cluster), and for sandbox/production setups.
Upfront, set an environment variable `ENV`, which is either `local`, `sandbox`, or `production`, e.g.
```bash
ENV=local && export ENV=$ENV
```
This will use the environment variables defined in `envs/${ENV}.env`.

### Deployment workflow: 'local' vs. 'sandbox' vs. 'production'
All features should be tested on `local` before deploying them to the `sandbox` environment
for further testing. `sandbox` is technically equivalent to `production`, but it does not contain any valuable
data and therefore it's perfectly fine to break and/or wipe it.
There, depending on the scope of the feature, and of the likeliness of breaking something,
another test with real data might be necessary.

Use common sense when deciding the scope of testing:
For instance, if you correct a typo in the webapp, you might well skip the sandbox testing.
In contrast, a new feature that changes the way data is processed should definitely be tested in the sandbox environment.

Only a well-tested feature should be deployed to production. Make sure a pull request is always self-contained
and 'shippable', i.e. deployment to production should be possible at any time.

### Initial deployment
All commands in this Readme assume you are in the root folder of the repository.
Please note that running and developing the alphakraken is only tested for MacOS and Linux
(the UI can be accessed from any OS, of course).

#### One-time initializations
The following steps are required for both the `local` and the `sandbox`/`production` deployments.
For the latter, additional steps are required, see [here](#additional-steps-required-for-initial-sandboxproduction-deployment).

1. Install [Docker](https://docs.docker.com/engine/install/ubuntu/), clone the repository into a folder and `cd` into it.

2. Set the current user as the user within the airflow containers and get the correct permissions on the "logs"
directory (otherwise, `root` would be used)
```bash
echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow
```

3. On the PC that will host the internal airflow database run
```bash
./compose.sh --profile infrastructure up airflow-init
```
Note: depending on your operating system and configuration, you might need to run `docker compose` command with `sudo`.

Now run the containers (see below).
The following initialization steps need to be done once the containers are up:

4. In the Airflow UI, set up the SSH connection to the cluster (see [below](#setup-ssh-connection)).
If you don't want to connect to the cluster, just create the connection of type
"ssh" and name "cluster_ssh_connection" with some dummy values for host, username, and password.
In this case, make sure to set the Airflow variable `debug_no_cluster_ssh=True` (see below).

5. In the Airflow UI, set up the required Pools (see [below](#setup-pools)).

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

See below for [some useful Docker commands](#some-useful-docker-commands).


### Additional steps required for initial sandbox/production deployment

The main differences between the `local` and the `sandbox`/`production` deployments are:
- `local` has all services running on the same machine within the same docker-compose network,
whereas `sandbox`/`production` is per default distributed over two machines
- `sandbox`/`production` needs additional steps to configure the cluster and the network bind mounts

The different services can be distributed over several machines. The only important thing is that there
it exactly one instance of each of the 'central components': `postgres-service`, `redis-service`, and `mongodb-service`.
One reasonable setup is to have the airflow infrastructure and the MongoDB service on one machine,
and all workers on another. This is the current setup in the docker-compose, which is reflected by the
profiles `infrastructure` and `workers`, respectively. If you move one of the central components
to another machine, you might need to adjust the `*_HOST` variables in the
`./env/${ENV}.env` files (see comments there). Of course, one machine could also host them all.

Make sure that the time is in sync between all machines, e.g. by using the same time server.

For production: set strong passwords for `AIRFLOW_PASSWORD`, `MONGO_PASSWORD`, and `POSTGRES_PASSWORD`
in `./env/production.env` and `MONGO_INITDB_ROOT_PASSWORD` in `./env/.env-mongo`.
Make sure they don't contain weird characters like '\' or '#' as they might interfere with name resolution in `docker-compose.yaml`.

#### On the PC (VM) hosting the airflow infrastructure [start-infrastructure]
0. `ssh` into the PC/VM and set `export ENV=sandbox` (`production`).

1. Set up the [pool bind mounts](#set-up-pool-bind-mounts) for `airflow_logs` only. Here, the logs of the individual task runs will be stored
for display in the Airflow UI.

2. Run the airflow infrastructure and MongoDB services
```bash
./compose.sh --profile infrastructure up --build -d
```
Then, access the Airflow UI at http://hostname:8080/ and the Streamlit webapp at http://hostname:8501/.

#### On the PC (VM) hosting the workers [start-worker]
0. `ssh` into the PC/VM and set `export ENV=sandbox` (`production`).

1. Set up the [pool bind mounts](#set-up-pool-bind-mounts)
and mount all instruments using the `./mountall.sh` command described in [instruments](#add-a-new-instrument).

2. Run the worker containers (sandbox/production version)
```bash
./compose.sh --profile workers up --build -d
```
which spins up on worker service for each instrument.

#### On the cluster
1. Log into the cluster using the `kraken` user.
2. Create this directory
```bash
mkdir -p ~/slurm/jobs
```
3. Copy the cluster run script `submit_job.sh` to `~/slurm`.
Make sure to update also this file when deploying a new version of the AlphaKraken.

4. Set up AlphaDIA  (see [below](#setup-alphadia)).

### General note on how Kraken gets to know the data

Each worker needs two 'views' on the pool backup and output data.

The first view enables read/write access,
by mounting on the Kraken host PC a specific (network) folder (e.g. `\\pool-backup\pool-backup` or `\\pool-output\pool-output`)
using `cifs` mounts (wrapped by `mountall.sh`)
to a target folder and then mapping this target folder to a worker container
in `docker-compose.yaml`.

The second view is the location of the data as seen from the cluster
(e.g. `/fs/pool/pool-backup` or `/fs/pool/pool-output`),
which is required to set the paths for the cluster jobs correctly.

For instruments, only the first type of view is required, as the cluster does not access the instruments directly.


### Set up pool bind mounts
The workers need bind mounts set up to the pool filesystems for backup and reading alphaDIA output data.
All airflow components (webserver, scheduler and workers) need a bind mount to a pool folder to read and write airflow logs.
Additionally, workers need one bind mount per instrument PC is needed (cf. section below).

0. (on demand) Install the `cifs-utils` package (otherwise you might get errors like
`CIFS: VFS: cifs_mount failed w/return code = -13`)
```bash
sudo apt install cifs-utils
```

1. Create folders `settings`, `output`, and `airflow_logs` in the desired pool location(s), e.g. under `/fs/pool/pool-alphakraken`.

2. Make sure the variables `MOUNTS_PATH`, `BACKUP_POOL_FOLDER` `QUANTING_POOL_FOLDER` in the `envs/${ENV}.env` file are set correctly.
Check also `MOUNTS_PATH`, `BACKUP_POOL_PATH` `QUANTING_POOL_PATH` in the `mountall.sh` script.

3. Mount the backup, output and logs folder. You will be asked for passwords.
```bash
./mountall.sh $ENV backup
./mountall.sh $ENV output
./mountall.sh $ENV logs
```

Note: for now, user `kraken` should only have read access to the backup pool folder, but needs `read/write` on the `output`
folder. If you need to remount one of the folders, add the `umount` option, e.g.
`./mountall.sh $ENV output umount`.

IMPORTANT NOTE: it is absolutely crucial that the mounts are set correctly (as provided by the `mountall.sh` script)
as the workers operate only on docker-internal paths and cannot verify the correctness of the mounts.

### Setup SSH connection
This connection is required to interact with the SLURM cluster.

1. Open the Airflow UI, navigate to "Admin" -> "Connections" and click the "+" button.
2. Fill in the following fields:
    - Connection Id: `cluster_ssh_connection`
    - Conn Type: `SSH`
    - Host: `<cluster_head_node_ip>`  # the IP address of a cluster head node, in this case `<cluster_head_node>`
    - Username: `<user name of kraken SLURM user>`
    - Password: `<password of kraken SLURM user>`
3. (optional) Click "Test" to verify the connection.
4. Click "Save".

### Setup required pools
Pools are used to limit the number of parallel tasks for certain operations. They are managed via the Airflow UI
and need to be created manually once.
1. Open the Airflow UI, navigate to "Admin" -> "Pools".
2. For each pool defined in `settings.py:Pools`, create a new pool with a sensible value (see suggestions in the `Pools` class).

### Setup alphaDIA on the cluster
For details on how to install alphaDIA on the SLURM cluster, follow the alphaDIA
[https://github.com/MannLabs/alphadia/blob/main/docs/installation.md#slurm-cluster-installation](Readme).

In a nutshell, to install a certain version, e.g. `VERSION=1.7.0`:
```bash
conda create --name alphadia-${VERSION} python=3.11 -y
```
```bash
conda activate alphadia-${VERSION}
```
```bash
pip install "alphadia[stable]==${VERSION}"
```
Make sure the environment is named `alphadia-${VERSION}`.
Also, don't forget to install `mono` (cf. alphaDIA Readme).

### Add a new instrument
Each instrument is identified by a unique `<INSTRUMENT_ID>`,
which should be lowercase and contain only letters and numbers but is otherwise arbitrary (e.g. "newinst1").
Note that some parts of the system rely on convention, so make sure to use exactly
`<INSTRUMENT_ID>` (case-sensitive!) in the below steps.

1. Create a folder named `Backup` in the folder where the acquired files are saved.

2. Add the following block to the end of `mountall.sh`:
```bash
if [ "${ENTITY}" == "<INSTRUMENT_ID>" ]; then
  USERNAME=<username for instrument>
  MOUNT_SRC=//<ip address of instrument>/<INSTRUMENT_ID>/$SUBFOLDER
  MOUNT_TARGET=${MOUNTS_PATH}/instruments/<INSTRUMENT_ID>
fi
```

3. Transfer this change to the AlphaKraken PC and execute
```
./mountall.sh $ENV <INSTRUMENT_ID>
```

4. In the `settings.py:INSTRUMENTS` dictionary, add a new entry by copying an existing one and adapting it like
```
    "<INSTRUMENT_ID>": {
        InstrumentKeys.TYPE: InstrumentTypes.<INSTRUMENT_TYPE>,
        # (there might be some keys here, just copy them)
    },
```
Here, `<INSTRUMENT_TYPE>` is one of the keys defined in the `InstrumentKeys` class and determines what output is expected from the instrument:
`THERMO` -> 1 `.raw` file, `BRUKER` -> `.d` folder, `ZENO` -> `.wiff` file plus more files with the same stem.


5. In `docker-compose.yaml`, add a new worker service, by copying an existing one and adapting it like:
```
  airflow-worker-<INSTRUMENT_ID>:
    <<: *airflow-worker
    command: celery worker -q kraken_queue_<INSTRUMENT_ID>
    # there might be additional keys here, just copy them
    volumes:
      - ${MOUNTS_PATH:?error}/airflow_logs:/opt/airflow/logs:rw
      - ${MOUNTS_PATH:?error}/output:/opt/airflow/mounts/output:ro
      - ${MOUNTS_PATH:?error}/instruments/<INSTRUMENT_ID>:/opt/airflow/mounts/instruments/<INSTRUMENT_ID>:ro
      - ${MOUNTS_PATH:?error}/backup/<INSTRUMENT_ID>:/opt/airflow/mounts/backup/<INSTRUMENT_ID>:rw
```
Make sure to replace each instance of `<INSTRUMENT_ID>` with the correct instrument ID
and to not accidentally drop the `ro` and `rw` flags as they limit file access rights.

6. Start the new container `airflow-worker-<INSTRUMENT_ID>`  (cf. [above](#start-worker)).

7. Restart all relevant containers (scheduler, file mover and remover) with the `--build` flag (cf. [above](#start-infrastructure)).

8. Open the airflow UI and unpause the new `*.<INSTRUMENT_ID>` DAGs. It might be wise to do this one after another,
(`instrument_watcher` -> `acquisition_handler` -> `acquisition_processor`.) and to check the logs for errors before starting the next one.


### Deploying new code versions
These steps need to be done on all machines that run alphakraken services.
Make sure the code is always consistent across all machines!
0. If in doubt, create a  backup copy of the `mongodb_data_$ENV` and `airflowdb_data_$ENV` folders (on the machine that hosts the DBs).
1. On each machine, pull the most recent version of the code from the repository using `git pull` and a personal access token.
2. Check if there are any special changes to be done (e.g. updating `submit_job.sh` on the cluster,
new mounts, new environment variables, manual database interventions, ..) and apply them.
3. (when deploying workers) To avoid copying processes being interrupted, in the Airflow UI set the size of the `file_copy_pool` to 0 and wait until all `copy_raw_file` tasks are finished.
4. Stop all docker compose services that need to be updated across all machines using the `./compose.sh --profile <some profile> stop` command.
5. Start all docker compose services again, first the `infrastructure` services, then the `workers` services.
6. Set the size of `file_copy_pool` to the number it was before.
7. Normal operation should be resumed after about 5 minutes. Depending on when they were shut down, some tasks
could be in an `error` state though. Check after a few hours if some files are stuck and resolve the issues with the Airflow UI.

## Local development

### Development setup
This is required to have all the required dependencies for local development, in order to enable your IDE
dereference all dependencies and to run the tests.
1. Set up your environment for developing locally with
```bash
PYTHON_VERSION=3.11
AIRFLOW_VERSION=2.9.3
git clone git@github.com:MannLabs/alphakraken.git
cd alphakraken
conda create --name alphakraken python=${PYTHON_VERSION} -y
conda activate alphakraken
pip install apache-airflow==${AIRFLOW_VERSION} --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

2. Install all requirements for running, developing and testing with
```bash
pip install -r airflow_src/requirements_airflow.txt
pip install -r shared/requirements_shared.txt
pip install -r webapp/requirements_webapp.txt
pip install -r requirements_development.txt
```

3. (optional) Mount the code directly into the containers. This way, changes are reflected immediately, without having to
rebuild or restart containers (this could be still necessary on certain changes though).
Locate the corresponding mappings in `docker-compose.yaml` (look for "Uncomment for local development") and uncomment them.

### Unit Tests
Run the tests with
```bash
python -m pytest
```
If you encounter a `sqlite3.OperationalError: no such table: dag`, run `airflow db init` once.

### Manual testing
This allows testing most of the functionality on your local machine. The SSH connection is cut off, and a
special worker ("test1") is used that has the `airflow_test_folder` mounted (instead of the pool).
Note: the instrument type for `test1` is set to `thermo`. In order to test other workflows,
change this locally in `settings.py:INSTRUMENTS`.

1. Run the `docker compose` (`./compose.sh`) command for the local setup (cf. above) and log into the airflow UI.
2. Unpause all `*.test1` DAGs. The "watcher" should start running.
3. If you do not want to feed the cluster, set the Airflow variable `debug_no_cluster_ssh=True` (see above)
4. In the webapp, create a project with the name `P123`, and add some fake settings to it.
5. Create a test raw file in the backup pool folder to fake the acquisition.

For type "Thermo":
```bash
I=$((I+1)); RAW_FILE_NAME=test_file_SA_P123_${I}.raw; echo $RAW_FILE_NAME
touch airflow_test_folders/instruments/test1/$RAW_FILE_NAME
```

For type "Zeno":
```bash
I=$((I+1)); RAW_FILE_STEM=test_file_SA_P123_${I}; RAW_FILE_NAME=$RAW_FILE_STEM.wiff; echo $RAW_FILE_NAME
touch airflow_test_folders/instruments/test1/$RAW_FILE_NAME
touch airflow_test_folders/instruments/test1/${RAW_FILE_STEM}.wiff2
touch airflow_test_folders/instruments/test1/${RAW_FILE_STEM}.wiff.scan
touch airflow_test_folders/instruments/test1/${RAW_FILE_STEM}.timeseries.data
```

For type "Bruker":
```bash
I=$((I+1)); RAW_FILE_NAME=test_file_SA_P123_${I}.d; echo $RAW_FILE_NAME
mkdir -p airflow_test_folders/instruments/test1/$RAW_FILE_NAME/some_folder
touch airflow_test_folders/instruments/test1/$RAW_FILE_NAME/analysis.tdf_bin
touch airflow_test_folders/instruments/test1/$RAW_FILE_NAME/analysis.tdf
touch airflow_test_folders/instruments/test1/$RAW_FILE_NAME/some_folder/some_file.txt
```

6. Wait until the `instrument_watcher` picks up the file (you may mark the `wait_for_new_files` task as "success" to speed up the process).
It should trigger a `acquisition_handler` DAG, which in turn should trigger a `acquisition_processor` DAG.

7. After the `compute_metrics` task failed because of missing output files,
create those by copying fake alphaDIA result data to the expected output directory
(set `YEAR_MONTH=<current year>_<current month>`, e.g. `2024_08`)
```bash
NEW_OUTPUT_FOLDER=airflow_test_folders/output/P1/$YEAR_MONTH/out_$RAW_FILE_NAME
mkdir -p $NEW_OUTPUT_FOLDER
cp airflow_test_folders/_data/* $NEW_OUTPUT_FOLDER
```
and clear the task state using the UI.

8. Wait until DAG run finished and check results in the webapp.

### Connect to the DB
Use e.g. MongoDB Compass to connect to the MongoDB running in Docker using the url `<hostname>:27017` (e.g. `localhost:27017`),
the credentials (defined in `envs/$ENV.env`) and make sure the "Authentication Database" is "`krakendb`".

#### Changing the DB 'schema'
Although MongoDB is schema-less in principle, the use of `mongoengine` enforces a schema-like structure.
In order to modify this structure of the DB (e.g. rename a field), you need to
1. Backup the DB by copying the `mongodb_data_$ENV` folder
2. Pause all DAGs and other services that may write to the DB
3. Connect to the DB using  MongoDB Compass and use the `update` button with a command like
```
{ $rename: { 'old_name': 'new_name' } }
```
4. Check that the operation was successful.
5. Deploy the new code that is compatible with the new schema.
6. Restart everything.

### pre-commit hooks
It is highly recommended to use the provided pre-commit hooks, as the CI pipeline enforces all checks therein to
pass in order to merge a branch.

The hooks need to be installed once by
```bash
pre-commit install
```
You can run the checks yourself using:
```bash
pre-commit run --all-files
```

### Type checking
The code can be type-checked by
```bash
pip install pytype==2024.10.11
pytype .
```

### A note on importing and PYTHONPATH
Airflow adds the folders `dags` and `plugins` to the `PYTHONPATH`
by default (cf. [here](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html#built-in-pythonpath-entries-in-airflow)).
To enable a consistent importing of modules, we need to do the same for the Streamlit webapp (done in the Dockerfile) and for `pytest` (done in `pyproject.toml`).

In addition, in order to import the `shared` module consistently, we need to add the root directory to the `PYTHONPATH`,
for Airflow (done in the Dockerfile), the Streamlit webapp (done in the Dockerfile), and for `pytest` (done in `pyproject.toml`).
Note: beware of name clashes when introducing new top-level packages in addition to `shared`, cf.
[here](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html#best-practices-for-your-code-naming).

To have your IDE recognize the imports correctly, you might need to take some action.
E.g. in PyCharm, you need to mark `dags`, `plugins`, `shared`, and `airflow_src` as "Sources Root".

### Run Airflow standalone (not actively maintained)
<details>
  <summary>deprecated</summary>
Alternatively, run airflow without Docker using
```bash
MONGO_USER=<mongo_user>
```
The login password to the UI is displayed in the logs below the line `Airflow is ready`.
You need to point the `dags_folder` variable in ` ~/airflow/airflow.cfg` to the absolute path of the `dags` folder.

Note that you will need to have a MongoDB running on the default port `27017`, e.g. by
`docker compose --env-file=envs/local.env run --service-ports mongodb-service`
Also, you will need to fire up the Streamlit webapp yourself by `docker compose --env-file=envs/local.env run -e MONGO_HOST=host.docker.internal --service-ports webapp`.

Note that currently, the docker version is recommended as the standalone version is not part of regular testing and
might not work as expected.
</details>

## Troubleshooting

See also  .

### Problem: worker does not start

A worker fails to start up with the error
```
Error response from daemon: Mounts denied:
The path /home/kraken-user/alphakraken/sandbox/mounts/.... is not shared from the host and is not known to Docker.
```

#### Solution
Check that the mounting has been done correctly. If the instrument is currently unavailable,
you can either ignore the error or temporarily comment out the corresponding worker definition in `docker-compose.yaml`.
Once the instrument is available again, uncomment the worker definition and restart the container.

Sometimes, substituting `--build` with `--build --force-recreate` in the `docker compose` command helps
resolve mounting problems.


### Problem: a lot of tasks are in the 'scheduled' state
This can be seen in the Airflow UI -> Admin -> Pools.
If "Scheduled Slots" is large, but "Running Slots" reasonable small (less than 32 per scheduler)
then it might help to give the scheduler a fresh start on the PC/VM hosting the infrastructure:
```bash
./compose.sh up airflow-scheduler --build --force-recreate -d
```

### Problem: a `copy_raw_file` task is stuck
Symptom: the `copy_raw_file` task gets stuck when checking whether the file
is already present on the pool backup. In addition, `ls` from the
PC hosting the worker on pool backup folder containing the file does not
return (from the cluster, it does).

#### Solution
Most likely, this is because the file copying got interrupted, e.g. due to manual restart of the worker.
To resolve, move the file from the backup pool folder and restart the `copy_raw_file` task.


## Useful commands

### Some useful MongoDB commands
Find all files for a given instrument with a given status that are younger than a given date
```
{ $and: [{status:"error"}, {instrument_id:"test2"}, {created_at_: {$gte: new ISODate("2024-06-27")}}]}
```

### Some useful Docker commands
Instead of referencing a profile (which can refer to multiple services), you can also interact with individual services:
```bash
./compose.sh down airflow-webserver
```

See state of containers
```bash
docker ps
```
To force kill a certain container, get its `ID` from the above command, do `ps ax | grep <ID>` to get the process ID, and then `kill -9 <PID>`.

See state of mounts
```bash
df
```

Watch logs for a given service (don't confuse with the Airflow logs)
```bash
./compose.sh logs airflow-worker-test1 -f
```

Start bash in a given service container
```bash
./compose.sh exec airflow-worker-test1 bash
```

Clean up all containers, volumes, and images
```bash
./compose.sh down --volumes  --remove-orphans --rmi
```

If you encounter problems with mounting or any sorts of caching issues, try to replace
`--build` with `--build --force-recreate`.

Restarting Docker
```
systemctl restart docker
```

### Cluster load management
For each acquired file, a processing job on the SLURM cluster will be scheduled. If, for any reason, the number of
concurrently submitted jobs should be limited, set the size of the `cluster_slots_pool`
(in the Airflow UI under "Admin" -> "Pools") accordingly. Note that this setting does not affect
jobs that have already been submitted and thus may take a while to take effect.

## Airflow Variables
These variables are set in the Airflow UI under "Admin" -> "Variables". They steer the behavior of the whole system,
so be careful when changing them. If in doubt, pause all DAGs that are not part of the current problem before changing them.

### min_free_space_gb (default: -1)
If set to a positive number (unit: GB), the `file_remover` will remove files (oldest first) from the
instrument backup folder until the free space is above this threshold.
The value of `min_file_age_to_remove_in_days` (see below) is taken into account when
selection files to remove.

Recommended setting in production: `300` (big enough to avoid running out of space over a weekend
(in the worst case scenario that the file_remover stops running on Friday).

### min_file_age_to_remove_in_days (default: 14)
The minimum file age in days for files to be removed by the file_remover.

Recommended setting in production: `14` (default)

### allow_output_overwrite (default: False)
If set to `True`, the system will overwrite existing output files. Convenience switch to avoid manual deletion of output files
in case something went wrong with the quanting.

Recommended setting in production: False (default)

### debug_no_cluster_ssh (default: False)
`debug_no_cluster_ssh` If set to `True`, the system will not connect to the SLURM cluster. This is useful for
testing, debugging and to avoid flooding the cluster at the initial setup.

Recommended setting in production: False (default)

### debug_max_file_age_in_hours (default: -1)
If set, files that are older will not be processed by the acquisition handler, i.e. not be backed up nor quanted.
They will nevertheless be added to the DB, with status="ignored".
Needs to be a valid float, "-1" to deactivate.

Recommended setting in production: -1 (default)
