# alphakraken
A new version of the Machine Kraken

## Docker
Start the docker containers with
```bash
docker-compose up
```


## Local development
Set up your environment for developing locally with
```bash
conda create --name alphakraken python=3.11 -y
conda activate alphakraken
pip install apache-airflow==2.9.1
```

Install requirements for developing with
```bash
pip install -r requirements_development.txt
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
