# alphakraken
A new version of the Machine Kraken.

Note: this Readme is relevant only for developers and administrators. Regular user should find all required documentation
in the [AlphaKraken WebApp](http://10.31.0.192:8501/).

## Deployment
This guide is both valid for a local setup (without connection to pool or cluster), and for sandbox/production setups.
Upfront, set a bash variable `ENV`, which is either `local`, `sandbox`, or `prod`, e.g.
```bash
ENV=local
```
This will use the environment variables defined in `envs/${ENV}.env`.

### Initializing and running the kraken
All commands in this Readme assume you are in the root folder of the repository.
Please note that running and developing the alphakraken is only tested for MacOS and Linux
(the UI can be accessed from any OS, of course).

#### One-time initializations
1. Install
[Docker Compose](https://docs.docker.com/engine/install/ubuntu/) and
[Docker](https://docs.docker.com/compose/install/linux/#install-using-the-repository), clone the repository into a folder and `cd` into it.

2. Set the current user as the user within the airflow containers and get the correct permissions on the "logs"
directory (otherwise, `root` would be used)
```bash
echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow
mkdir airflow_logs
```

3. Initialize the internal airflow database:
```bash
docker compose --env-file=envs/.env-airflow --env-file=envs/${ENV}.env run airflow-init
```
Note: depending on your operating system and configuration, you might need to run `docker compose` command with `sudo`.


4. In the Airflow UI, set up the SSH connection to the cluster (see [below](#setup-ssh-connection)).
If you don't want to connect to the cluster, just create the connection of type
"ssh" and name "cluster_ssh_connection" with some dummy values for host, username, and password.
In this case, make sure to set the Airflow variable `debug_no_cluster_ssh=True` (see below).

5. In the Airflow UI, set up the required Pools (see [below](#setup-pools)).

#### Run the containers (local version)
Start all docker containers (but without the workers mounted to the production file systems)
```bash
docker compose --env-file=envs/.env-airflow --env-file=envs/${ENV}.env up --build -d
```
After startup, the airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the Streamlit webapp on http://localhost:8501/ .

#### Some useful commands:
See state of containers
```bash
docker ps
```

Watch logs for a given service (omit the last part to see all logs)
```bash
docker compose --env-file=envs/${ENV}.env logs -f airflow-worker-test1
```

Start bash in a given service container
```bash
docker compose --env-file=envs/${ENV}.env exec airflow-worker-test1 bash
```

Clean up all containers, volumes, and images (WARNING: database will be lost!)
```bash
docker compose --env-file=envs/${ENV}.env down --volumes  --remove-orphans --rmi
```

### Additional steps required for the sandbox/production setup

#### On the cluster
1. Create this directory
```bash
mkdir -p ~/slurm/jobs
```
and copy the cluster run script `submit_job.sh` to `~/slurm`. Make sure to update it on changes.

2. Set up alphaDIA  (see [below](#setup-alphadia)).

#### On the kraken PC
1. Set up the network bind mounts (see [below](#set-up-network-bind-mounts)) .

2. Run the containers (sandbox/production version)
```bash
docker compose --env-file=envs/.env-airflow --env-file=envs/${ENV}.env --profile all-workers up --build -d
```
which spins up additional worker containers for each instrument.

Then, access the Airflow UI at http://10.31.0.192:8081/ and the Streamlit webapp at http://10.31.0.192:8502/.


### General note on how Kraken gets to know the data
Each worker needs two 'views' on the data: the first one enables direct access to it,
by mounting a specific folder to a target on the kraken PC and then mounting the target
to a worker container. The second one is the location of the data as seen from the cluster,
this is required to set the paths for the cluster jobs correctly.

### Set up network bind mounts
(TODO describe persistent mount)
We need bind mounts set up to each backup pool folder, and to the project pool folder.
Additionally, one bind mount per instrument PC is needed (cf. section below).

1. Mount the backup pool folder
```bash
./mountall.sh $ENV backup
```

2. Mount the output folder
```bash
./mountall.sh $ENV output
```

Note: for now, user `kraken` should only have read access to the backup pool folder, but needs `read/write` on the `output`
folder.

Cf. also the environment variables `MOUNTS_PATH` and `IO_POOL_FOLDER` in the `envs/${ENV}.env` file.
If you need to remount one of the folders, add the `umount` option, e.g.
`./mountall.sh $ENV output umount`.

### Add a new instrument
Each instrument is identified by a unique `<INSTRUMENT_ID>`,
which should be lowercase and contain only letters and numbers but is otherwise arbitrary (e.g. "newinst1").
Note that some parts of the system rely on convention, so make sure to use exactly
`<INSTRUMENT_ID>` (case-sensitive!) in the below steps.

1. Add the following block to the end of `mountall.sh`:
```bash
if [ "${ENTITY}" == "<INSTRUMENT_ID>" ]; then
  USERNAME=<username for instrument>
  MOUNT_SRC=//<ip address of instrument>/<INSTRUMENT_ID>
  MOUNT_TARGET=${MOUNTS}/instruments/<INSTRUMENT_ID>
fi
```

2. Execute
```
./mountall.sh $ENV <INSTRUMENT_ID>
```

3. Create an output folder for the instrument
```bash
mkdir -p ${MOUNTS}/output/<INSTRUMENT_ID>
```

4. In the `settings.py:INSTRUMENTS` dictionary, add a new entry by copying an existing one and adapting it like
```
    "<INSTRUMENT_ID>": {
        # (there might be some keys here, just copy them)
    },
```

5. In `docker-compose.yml`, add a new worker service, by copying an existing one and adapting it like:
```
  airflow-worker-<INSTRUMENT_ID>:
    <<: *airflow-worker
    command: celery worker -q kraken_queue_<INSTRUMENT_ID>
    # there might be additional keys here, just copy them
```

6. Restart all containers with the `--build` flag (cf. [above](#on-the-kraken-pc)).

7. (optional) Without any further intervention, the kraken would now process all files on the new instrument. If this is
not desired, you may temporarily set the `max_file_age_in_hours` Airflow variable (see below) to process only
recent files. Older ones will then be added to the DB with status 'ignored'. Don't forget to set it back to the original value as this is a global setting.

8. Open the airflow UI and unpause the new `*.<INSTRUMENT_ID>` DAGs. It might be wise to do this one after another,
(`instrument_watcher` -> `acquisition_handler` -> `acquisition_processor`.) and to check the logs for errors before starting the next one.

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
and need to be created manually one.
1. Open the Airflow UI, navigate to "Admin" -> "Pools".
2. For each pool defined in `settings.py:Pools`, create a new pool with a sensible value (see suggestions in the `Pools` class).

### Setup alphaDIA
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
pip  install "alphadia[stable]==${VERSION}"
```
Make sure the environment is named `alphadia-$VERSION`.
Also, don't forget to install `mono` (cf. alphaDIA Readme).


## Local development

### Deployment workflow: 'local' vs. 'sandbox' vs. 'production'
All features should be tested locally before deploying them to the sandbox environment
(which is technically equivalent to the production).
There, depending on the scope of the feature, and of the likeliness of breaking something,
another test with real data might be necessary.
For instance, if you correct a typo in the webapp, you might well skip the sandbox testing.
In contrast, a new feature that changes the way data is processed should definitely be tested in the sandbox environment.

Only a well-tested feature should deployed to production.

### Development setup
This is required to have all the required dependencies for local deployment and testing.
1. Set up your environment for developing locally with
```bash
PYTHON_VERSION=3.11
AIRFLOW_VERSION=2.9.2
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
special worker ("test1") is used that is connected to the `airflow_test_folder` (not to the pool).

1. Run the `docker compose` command for the local setup (cf. above) and log into the airflow UI.
2. Unpause all `*.test1` DAGs. The "watcher" should start running.
3. If you do not want to feed the cluster, set the Airflow variable `debug_no_cluster_ssh=True` (see above)
4. In the webapp, create a project with the name `P1`, and add some fake settings to it.
5. Create a test raw file in the backup pool folder to fake the acquisition
```bash
I=$((I+1)); NEW_FILE_NAME=test_file_SA_P1_${I}.raw; echo $NEW_FILE_NAME
touch airflow_test_folders/instruments/test1/Backup/$NEW_FILE_NAME
```

6. Wait until the `instrument_watcher` picks up the file (you may mark the `wait_for_new_files` task as "success" to speed up the process).
It should trigger a `acquisition_handler` DAG, which in turn should trigger a `acquisition_processor` DAG.

7. After the `compute_metrics` task failed because of missing output files,
create those by copying fake alphaDIA result data to the expected output directory
```bash
NEW_OUTPUT_FOLDER=airflow_test_folders/output/P1/out_$NEW_FILE_NAME
mkdir -p $NEW_OUTPUT_FOLDER
cp airflow_test_folders/_data/stat.tsv $NEW_OUTPUT_FOLDER
```
and clear the task state using the UI.

8. Wait until DAG run finished and check results in the webapp.

### Connect to the DB
Use e.g. MongoDB Compass to connect to the MongoDB running in Docker using the url `localhost:27017`,
the credentials (e.g. defined in `envs/local.env`) and make sure the "Authentication Database" is "krakendb".

#### Changing the DB 'schema'
Although MongoDB is schema-less in principle, the use of `mongoengine` enforces a schema-like structure.
In order to modify this structure of the DB (e.g. rename a field), you need to
1. Backup the DB
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

## Troubleshooting

### Problem: worker does not start

A worker fails to start up with the error
```
Error response from daemon: Mounts denied:
The path /home/kraken-user/alphakraken/sandbox/mounts/.... is not shared from the host and is not known to Docker.
```

#### Solution
Check that the mounting has been done correctly. If the instrument is currently unavailable,
you can either ignore the error or temporarily comment out the corresponding worker definition in `docker-compose.yml`.
Once the instrument is available again, uncomment the worker definition and restart the container.

Sometimes, substituting `--build` with `--build --force-recreate` in the `docker compose` command helps
resolve mounting problems.

### Restarting Docker
```
sudo systemctl restart docker
```

### Some useful MongoDB commands
Find all files for a given instrument with a given status that are younger than a given date
```
{ $and: [{status:"error"}, {instrument_id:"test2"}, {created_at_: {$gte: new ISODate("2024-06-27")}}]}
```

## Airflow Variables
These variables are set in the Airflow UI under "Admin" -> "Variables". They steer the behavior of the whole system,
so be careful when changing them. If in doubt, pause all DAGs that are not part of the current problem before changing them.

### max_file_age_in_hours
If set, files that are older will not be processed by the acquisition handler.
They will nevertheless be added to the DB, with status="ignored".
Needs to be a valid float, "-1" to deactivate.
This is useful to avoid processing of older files if a new instrument is attached
or after a long downtime.

Recommended setting in production: -1 (default)

### allow_output_overwrite
If set to `True`, the system will overwrite existing output files. Convenience switch to avoid manual deletion of output files
in case something went wrong with the quanting.

Recommended setting in production: False (default)

### debug_no_cluster_ssh
`debug_no_cluster_ssh` If set to `True`, the system will not connect to the SLURM cluster. This is useful for
testing, debugging and to avoid flooding the cluster at the initial setup.

Recommended setting in production: False (default)
