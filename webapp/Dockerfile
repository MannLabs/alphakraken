# https://docs.streamlit.io/deploy/tutorials/docker
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
COPY webapp/requirements_webapp.txt requirements_webapp.txt

RUN  --mount=type=cache,target=/root/.cache/pip \
     pip install -r requirements.txt \
     && pip install -r requirements_webapp.txt

COPY webapp webapp
# shared and plugins are directories that contain modules that are required by the webapp
COPY airflow_src/plugins webapp/plugins
COPY shared webapp/shared

# cf. README.md:A note on importing
ENV PYTHONPATH "${PYTHONPATH}:/app/webapp/plugins"
ENV PYTHONPATH "${PYTHONPATH}:/app/webapp/shared"

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "webapp/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
