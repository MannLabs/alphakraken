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

Install requirements for developing and testing with
```bash
pip install -r requirements_development.txt
```

Start the docker containers providing an all-in-one solution with
```bash
docker compose up
```
The airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the streamlit app on http://localhost:8501/ .

Alternatively, run airflow without Docker using
```bash
MONGO_USER=<mongo_user>
```
The login password to the UI is displayed in the logs below the line `Airflow is ready`.
You need to point the `dags_folder` variable in ` ~/airflow/airflow.cfg` to the absolute path of the `dags` folder.

Note that you will need to have a MongoDB running on the default port `27017`, e.g. by
`docker compose run --service-ports mongodb-service`
Also, you will need to fire up the streamlit app yourself by `docker compose run -e MONGO_USER=<mongo_user>

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
4. Wait until it appears in the streamlit UI.

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
To enable a consistent importing of modules, we need to do the same for the streamlit app (done in the Dockerfile) and for `pytest` (done in `pyproject.toml`).

In addition, in order to import the `shared` module consistently, we need to add the root directory to the `PYTHONPATH`,
for Airflow (done in the Dockerfile), the streamlit app (done in the Dockerfile), and for `pytest` (done in `pyproject.toml`).
Note: beware of name clashes when introducing new top-level packages in addition to `shared`, cf.
[here](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html#best-practices-for-your-code-naming).

To have your IDE recognize the imports correctly, you might need to take some action.
E.g. in PyCharm, you need to mark `dags`, `plugins` and `shared` as "Sources Root".

## Deployment
### Initial setup of Kraken PC
1. Install
[Docker Compose](https://docs.docker.com/engine/install/ubuntu/) and
[Docker](https://docs.docker.com/compose/install/linux/#install-using-the-repository).
2. Clone the repository into `/home/kraken-user/alphakraken/sandbox/alphakraken`.
3. `cd` into this directory and execute `echo -e "AIRFLOW_UID=$(id -u)" > .env` to set the current user as the user
within the airflow containers (otherwise, `root` would be used).
4. Set up the network bind mounts (see below).
5. Run `docker compose up -d` to start the services.
6. Access the Airflow UI at `http://<kraken_pc_ip>:8080/` and the Streamlit app at `http://<kraken_pc_ip>:8501/`.

#### Some useful commands:
See state of containers
```bash
docker ps
```

Watch logs for a given service (omit the last part to see all logs)
```bash
docker compose logs -f streamlit-app
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
e.g. at  `/home/kraken-user/alphakraken/sandbox/mounts/ms14/Test2`.

2. In `docker-compose.yml`, locate the `# ADD INSTRUMENTS HERE` comment and add a new entry for the instrument:
```
# Test2 on ms14:
- /home/kraken-user/alphakraken/sandbox/mounts/ms14/Test2:/opt/airflow/acquisition_pcs/astral_1
```
where `astral_1` can be freely chosen as long as it is unique.

3. In the `settings.py:INSTRUMENTS` dictionary, add a new entry
```
    "test2": {
        InstrumentKeys.RAW_DATA_PATH: "astral_1",
    },
```

4. Shut down and restart the containers with `docker compose down` and `docker compose up -d`.

5. Open the airflow UI and unpause the new `*.test2` DAGs.
