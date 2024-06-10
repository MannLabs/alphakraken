# alphakraken
A new version of the Machine Kraken

## Local development
Set up your environment for developing locally with
```bash
PYTHON_VERSION=3.11
AIRFLOW_VERSION=2.9.1
conda create --name alphakraken python=${PYTHON_VERSION} -y
conda activate alphakraken
pip install apache-airflow==${AIRFLOW_VERSION} --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Install all requirements for running, developing and testing with
```bash
pip install -r airflow_src/requirements_airflow.txt
pip install -r shared/requirements_shared.txt
pip install -r webapp/requirements_webapp.txt
pip install -r requirements_development.txt
```

Start the docker containers providing an all-in-one solution with
```bash
docker compose up --build
```
The airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the Streamlit webapp on http://localhost:8051/ .

Alternatively, run airflow without Docker using
```bash
MONGO_USER=<mongo_user>
```
The login password to the UI is displayed in the logs below the line `Airflow is ready`.
You need to point the `dags_folder` variable in ` ~/airflow/airflow.cfg` to the absolute path of the `dags` folder.

Note that you will need to have a MongoDB running on the default port `27017`, e.g. by
`docker compose run --service-ports mongodb-service`
Also, you will need to fire up the Streamlit webapp yourself by `docker compose run -e MONGO_USER=<mongo_user>

Note that currently, the docker version is recommended.

### Unit Tests
Run the tests with
```bash
python -m pytest
```
If you encounter a `sqlite3.OperationalError: no such table: dag`, run `airflow db init` once.

### Manual testing
1. Run the `docker compose` command above and log into the airflow UI.
2. Unpause all DAGs. The "watchers" should start running.
3. Create a test file: `I=$((I+1)); touch test_folders/acquisition_pcs/apc_tims_1/test_file_${I}.raw`
4. Wait until it appears in the webapp.

### Connect to the DB
Use e.g. MongoDB Compass to connect to the MongoDB running in Docker using the url `localhost:27017`,
the credentials (e.g. defined in `envs/.dev.env`) and make sure the "Authentication Database" is "krakendb".

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
3. `cd` into this directory and execute `echo -e "AIRFLOW_UID=$(id -u)" > .env` to set the current user as the user
within the airflow containers (otherwise, `root` would be used).
4. Set up the network bind mounts (see below).
5. Run `docker compose up --build -d` to start the services.
6. Access the Airflow UI at `http://<kraken_pc_ip>:8080/` and the Streamlit webapp at `http://<kraken_pc_ip>:8051/`.


### Run production setup
In order for the production setup to run, you need to execude the command
```bash
docker compose up --build --profile prod-workers -d
```

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

### Set up network bind mounts
1. Create the mount target directories:
```bash
mkdir -p /home/kraken-user/alphakraken/sandbox/mounts/ms14
```
2. Mount the network drives (TODO describe persistent mount):
```bash
sudo mount -t cifs -o username=krakenuser //samba-pool-backup/pool-backup /home/kraken-user/alphakraken/sandbox/mounts/ms14
```
Note: for now, user `krakenuser` should only have read access to the pool folder.

### Add a new instrument
1. Mount the network drive as described above such that the new instrument's files are accessible,
e.g. at  `/home/kraken-user/alphakraken/sandbox/mounts/ms14/Test2` (this will be referenced as `<SOURCE_MOUNT>` below).

2. In `docker-compose.yml`, add a new worker service, by copying an existing one and adapting it like:
```
  airflow-worker-<INSTRUMENT_ID>:
    <<: *airflow-worker
    command: celery worker -q kraken_queue_<INSTRUMENT_ID>
    volumes:
      - ${AIRFLOW_PROJ_DIR:-./airflow_src}/../airflow_logs:/opt/airflow/logs
      - <SOURCE_MOUNT>:/opt/airflow/acquisition_pcs/<INSTRUMENT_ID>
    # (there might be additional keys here, just copy them)
```


3. In the `settings.py:INSTRUMENTS` dictionary, add a new entry by copying an existing one and adapting it like
```
    "<INSTRUMENT_ID>": {
        InstrumentKeys.RAW_DATA_PATH: "<INSTRUMENT_ID>",
        # (there might be additional keys here, just copy them)
    },
```

4. Shut down the containers with `docker compose down` and restart them (cf. above).

5. Open the airflow UI and unpause the new `*.<INSTRUMENT_ID>` DAGs.


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
