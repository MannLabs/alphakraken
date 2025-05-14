# https://docs.streamlit.io/deploy/tutorials/docker
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY shared/requirements_shared.txt requirements_shared.txt
COPY webapp/requirements_webapp.txt requirements_webapp.txt

RUN  --mount=type=cache,target=/root/.cache/pip \
     pip install -r requirements_shared.txt \
     && pip install -r requirements_webapp.txt

COPY webapp webapp
COPY shared webapp/shared
COPY webapp/.streamlit .streamlit

# cf. README.md:A note on importing
ENV PYTHONPATH "${PYTHONPATH}:/app/webapp/"

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# TODO: make this a volume mapping to not bake this into the image
RUN mkdir -p /opt/airflow/envs
COPY envs/alphakraken.*.yaml /opt/airflow/envs
# avoid sensitive data in the container
RUN sed -i '/\(username\|password\)/d'  /opt/airflow/envs/alphakraken.*.yaml

ENTRYPOINT ["streamlit", "run", "webapp/webapp.py", "--server.port=8501", "--server.address=0.0.0.0"]
