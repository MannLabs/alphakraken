# alphakraken
A new version of the Machine Kraken

## Local development

### Running the kraken (local version)
First, run a one-time initialization of the internal airflow database:
```bash
docker compose --env-file=envs/local.env run airflow-init
```

Start the docker containers providing an all-in-one solution with
```bash
docker compose --env-file=envs/local.env up --build
```
After startup, the airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the Streamlit webapp on http://localhost:8501/ .


### Development setup
1. Set up your environment for developing locally with
```bash
PYTHON_VERSION=3.11
AIRFLOW_VERSION=2.9.1
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

### Unit Tests
Run the tests with
```bash
python -m pytest
```
If you encounter a `sqlite3.OperationalError: no such table: dag`, run `airflow db init` once.

### Manual testing
1. Run the `docker compose` command above and log into the airflow UI.
2. Unpause all DAGs. The "watchers" should start running.
3. If you do not want to feed the cluster, set the Airflow variable `debug_no_cluster_ssh=True` (see below)
4. Create a test file and copy fake alphaDIA result data to the expected output directory:
```
I=$((I+1)); NEW_FILE_NAME=test_file_${I}.raw; echo $NEW_FILE_NAME
touch airflow_test_folders/backup_pool/test1/$NEW_FILE_NAME
NEW_OUTPUT_FOLDER=airflow_test_folders/output/out_$NEW_FILE_NAME
mkdir $NEW_OUTPUT_FOLDER
cp airflow_test_folders/_data/stat.tsv $NEW_OUTPUT_FOLDER
```
5. Wait until the `acquisition_watchers` picks up the file (you may mark the `wait_for_new_files` task as "success" to speed up the process).
6. Wait until it appears in the webapp.

### Connect to the DB
Use e.g. MongoDB Compass to connect to the MongoDB running in Docker using the url `localhost:27017`,
the credentials (e.g. defined in `envs/local.env`) and make sure the "Authentication Database" is "krakendb".

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

## Deployment
### Initial setup of Kraken PC
1. Install
[Docker Compose](https://docs.docker.com/engine/install/ubuntu/) and
[Docker](https://docs.docker.com/compose/install/linux/#install-using-the-repository).
2. Clone the repository into `/home/kraken-user/alphakraken/sandbox/alphakraken`.
3. `cd` into this directory and execute
```bash
echo -e "AIRFLOW_UID=$(id -u)" > envs/.env-airflow
mkdir airflow_logs
```
to set the current user as the user within the airflow containers (otherwise, `root` would be used)
and to have correct permissions for the logs directory.

4. Set up the network bind mounts (see below).

5. On the cluster:
```bash
mkdir -p ~/slurm/jobs
```
and copt the cluster run script `submit_job.sh` to `~/slurm`. Make sure to update it on changes.

6. Run one-time initialization of the internal airflow database:
```bash
docker compose --env-file=envs/prod.env run airflow-init
```

### Running the kraken (production)
In order for the production setup to run, you need to execute the command
```bash
docker compose --env-file=envs/.env-airflow --env-file=envs/prod.env up --build --profile prod-workers -d
```
Then, access the Airflow UI at `http://<kraken_pc_ip>:8081/` and the Streamlit webapp at `http://<kraken_pc_ip>:8502/`.

Note: after the first full startup it is currently required to make all files owned
by the kraken user: `sudo chown -R kraken:kraken *` (needs to be done only once).

#### Some useful commands:
See state of containers
```bash
docker ps
```

Watch logs for a given service (omit the last part to see all logs)
```bash
docker compose logs -f airflow-worker
```

Start bash in a given service container
```bash
docker compose exec airflow-worker bash
```

Clean up all containers, volumes, and images (WARNING: data will be lost!)
```bash
docker compose down --volumes  --remove-orphans --rmi all
```

### General note on how Kraken gets to know the data
Each worker needs two 'views' on the data: the first one enables direct access to it,
by mounting a specific folder to a target on the kraken PC and then mounting the target
to a worker container. The second one is the location of the data as seen from the cluster,
this is required to set the paths for the cluster jobs correctly.

### Set up network bind mounts
(TODO describe persistent mount)
We need bind mounts set up to each backup pool folder, and to the project pool folder.
Additionally, one bind mount per instrument PC is needed (cf. section below).

1. Create the mount target directories:
```bash
MOUNTS=/home/kraken-user/alphakraken/sandbox/mounts
mkdir -p ${MOUNTS}/pool-backup
mkdir -p ${MOUNTS}/output
```

2. Mount the backup pool folder:
```bash
sudo mount -t cifs -o username=kraken //samba-pool-backup/pool-backup ${MOUNTS}/pool-backup
```

3. Mount the project pool folder:
```bash
IO_POOL_FOLDER=//samba-pool-projects/pool-projects/alphakraken_test
sudo mount -t cifs -o username=kraken ${IO_POOL_FOLDER}/output ${MOUNTS}/output
```

Note: for now, user `kraken` should only have read access to the backup pool folder, but needs `read/write` on the `${MOUNTS}/output`.



### Add a new instrument
Each instrument is identified by a unique `<INSTRUMENT_ID>`,
which should be lowercase and contain only letters and numbers but is otherwise arbitrary (e.g. "test2").

1. Mount the instrument

Not needed until alphakraken takes over also the file transfer from acquisition PCS to backup pool.
<details>
  <summary>Mounting instruments (currently not needed)</summary>
Mount the instrument
```bash
MOUNTS=/home/kraken-user/alphakraken/sandbox/mounts
INSTRUMENT_TARGET=${MOUNTS}/instruments/<INSTRUMENT_ID>
mkdir -p ${INSTRUMENT_TARGET}
sudo mount -t cifs -o username=kraken ${APC_SOURCE} ${INSTRUMENT_TARGET}
```
where `${APC_SOURCE}` is the network folder of the APC. --
</details>

2. Add the location of the instrument data to the .env files in the `envs` folder
by creating a new variable `INSTRUMENT_PATH_<INSTRUMENT_ID>` (all upper case), e.g.
`INSTRUMENT_PATH_NEWINST1`:
```bash
INSTRUMENT_PATH_NEWINST1=some/relative/path/to/new_instrument
```
and add this new variable to `docker-compose.yml:x-airflow-common.environment`
```bash
INSTRUMENT_PATH_NEWINST1=${INSTRUMENT_PATH_NEWINST1:?error}
```

3. In `docker-compose.yml`, add a new worker service, by copying an existing one and adapting it like:
```
  airflow-worker-<INSTRUMENT_ID>:
    <<: *airflow-worker
    command: celery worker -q kraken_queue_<INSTRUMENT_ID>
```

4. In the `settings.py:INSTRUMENTS` dictionary, add a new entry by copying an existing one and adapting it like
```
    "<INSTRUMENT_ID>": {
        InstrumentKeys.RAW_DATA_PATH: get_env_variable(
            "INSTRUMENT_PATH_NEWINST1", "n_a"
        ),
        # (there might be additional keys here, just copy them)
    },
```

5. Shut down the containers with `docker compose down` and restart them (cf. above).

6. Open the airflow UI and unpause the new `*.<INSTRUMENT_ID>` DAGs.


### Setup SSH connection
This connection is required to interact with the SLURM cluster.

1. Open the Airflow UI, navigate to "Admin" -> "Connections" and click the "+" button.
2. Fill in the following fields:
    - Connection Id: `cluster-conn`
    - Conn Type: `SSH`
    - Host: `<cluster_head_node_ip>`  # the IP address of a cluster head node, in this case `<cluster_head_node>`
    - Username: `<user name of kraken SLURM user>`
    - Password: `<password of kraken SLURM user>`
3. (optional) Click "Test" to verify the connection.
4. Click "Save".

### Setup alphaDIA
For details on how to install alphaDIA on the SLURM cluster, follow the alphaDIA
[https://github.com/MannLabs/alphadia/blob/main/docs/installation.md#slurm-cluster-installation](Readme).

In a nutshell, to install a certain version, e.g. 1.6.2:
```bash
conda create --name alphadia-1.6.2 python=3.11 -y
```
```bash
conda activate alphadia-1.6.2
```
```bash
pip  install "alphadia[stable]==1.6.2"
```
Make sure the environment is named `alphadia-$VERSION`.
Also, don't forget to install `mono` (cf. alphaDIA Readme).


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


## Airflow Variables
These variables are set in the Airflow UI under "Admin" -> "Variables". They steer the behavior of the whole system,
so be careful when changing them. If in doubt, pause all DAGs that are not part of the current problem before changing them.

### debug_no_cluster_ssh
`debug_no_cluster_ssh` If set to `True`, the system will not connect to the SLURM cluster. This is useful for
testing, debugging and to avoid flooding the cluster at the initial setup.
