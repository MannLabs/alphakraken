# https://airflow.apache.org/docs/docker-stack/build.html
ARG AIRFLOW_VERSION=2.9.1

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

COPY ../requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
