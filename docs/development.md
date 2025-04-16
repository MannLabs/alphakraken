
## Local development

### Development setup
This is required to have all the required dependencies for local development, in order to enable your IDE
dereference all dependencies and to run the tests.
1. Set up your environment for developing locally with
```bash
PYTHON_VERSION=3.11
AIRFLOW_VERSION=2.10.5
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

#### Update coverage badge
```bash
pytest --cov .
coverage-badge > coverage.svg
```

### Local testing
This allows testing most of the functionality on your local machine. The SSH connection is cut off, and a
special worker ("test1") is used that has the `local_test` folder mounted (instead of the pool folders).

1. Run the `docker compose` (`./compose.sh`) command for the local setup (cf. above) and log into the Airflow UI.
2. (one-time setup) To decouple from the Slurm cluster, set the Airflow variable `debug_no_cluster_ssh=True` (see above).
Also, create the `cluster_slots_pool` and the `file_copy_pool` (cf. [here](#setup-required-pools)) in the Airflow UI.
In the webapp, create a project with the name `P123`, and add some fake settings to it.

3. Unpause all `*.test*` DAGs. The `instrument_watcher`s should start running.

4. Create a test raw file in the backup pool folder to fake the acquisition and data processing
```bash
local_test/create_test_run.sh test1
```
here, `test1` is a "Thermo"-type instrument. For other instruments, use `test2` (Bruker) or `test3` (Sciex).

5. Wait until the `instrument_watcher` picks up the file (you may mark the `wait_for_new_files` task as "success" to speed up the process)
and triggers an `acquisition_handler` DAG. Here again, you can mark the `monitor_acquisition` task as "success" to speed up the process.

6. Wait until DAG run finished and check results in the webapp.

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
