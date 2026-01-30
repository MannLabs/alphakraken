# https://airflow.apache.org/docs/docker-stack/build.html
ARG AIRFLOW_VERSION=2.11.0

FROM apache/airflow:${AIRFLOW_VERSION}

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# add root directory to the PYTHONPATH to enable importing the 'shared' module
# cf. README.md:A note on importing
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

COPY airflow_src/requirements_airflow.txt /
COPY ../shared/requirements_shared.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements_airflow.txt  \
    -r /requirements_shared.txt


# Note that in docker-compose.yaml you can choose to import DAGs and plugins via volume mounts,
# which makes code changes active immediately
COPY airflow_src/dags /opt/airflow/dags
COPY airflow_src/plugins /opt/airflow/plugins
# COPY airflow_src/config /opt/airflow/config  # comment in once config folder is being used
COPY shared /opt/airflow/shared

# TODO: make this a volume mapping to not bake this into the image
RUN mkdir -p /opt/airflow/envs
COPY envs/alphakraken.*.yaml /opt/airflow/envs
# avoid sensitive data in the container
RUN sed -i '/\(username\|password\)/d'  /opt/airflow/envs/alphakraken.*.yaml
