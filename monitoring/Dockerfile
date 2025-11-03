FROM python:3.11-slim-bookworm

WORKDIR /app

COPY shared/requirements_shared.txt requirements_shared.txt
COPY monitoring/requirements_monitoring.txt requirements_monitoring.txt


RUN  --mount=type=cache,target=/root/.cache/pip \
     pip install -r requirements_shared.txt \
     && pip install -r requirements_monitoring.txt


COPY monitoring/ .
COPY shared ./shared

# Copy YAML configuration files
RUN mkdir -p /opt/airflow/envs
COPY envs/alphakraken.*.yaml /opt/airflow/envs/
# Remove sensitive data (usernames/passwords) from the config
RUN sed -i '/\(username\|password\)/d' /opt/airflow/envs/alphakraken.*.yaml

CMD ["python", "main.py"]
