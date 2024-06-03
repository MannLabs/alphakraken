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
and open the webserver at `http://localhost:8080/`.

Alternatively, run airflow without Docker using
```bash
airflow standalone
```

Note that you will need to have a MongoDB running on the default port `27017`.


### Tests
Run the tests with
```bash
python -m pytest
```
If you encounter a `sqlite3.OperationalError: no such table: dag`, run `airflow db init` once.

### Connect to the DB
Use e.g. MongoDB Compass to connect to the MongoDB running in Docker using the url `localhost:27017`.

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
