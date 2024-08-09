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
docker-compose up
```
The airflow webserver runs on http://localhost:8080/ (default credentials: `airflow`/`airflow`), the streamlit app on http://localhost:8051/ .

Alternatively, run airflow without Docker using
```bash
MONGO_USER=<mongo_user>
```
The login password to the UI is displayed in the logs below the line `Airflow is ready`.
You need to point the `dags_folder` variable in ` ~/airflow/airflow.cfg` to the absolute path of the `dags` folder.

Note that you will need to have a MongoDB running on the default port `27017`, e.g. by
`docker-compose run --service-ports mongodb-service`
Also, you will need to fire up the streamlit app yourself by `docker-compose run -e MONGO_USER=<mongo_user>

Note that currently, the docker version is recommended.

### Unit Tests
Run the tests with
```bash
python -m pytest
```
If you encounter a `sqlite3.OperationalError: no such table: dag`, run `airflow db init` once.

### Manual testing
1. Run the docker-compose command above and log into the airflow UI.
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
