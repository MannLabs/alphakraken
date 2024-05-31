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

Run airflow with
```bash
airflow standalone
```
and open the webserver at `http://localhost:8080/`.


Run airflow with
```bash
python -m pytest
```
and open the webserver at `http://localhost:8080/`.

### Docker
Start the docker containers with
```bash
docker-compose up
```


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
