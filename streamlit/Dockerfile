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
COPY streamlit/requirements_streamlit.txt requirements_streamlit.txt

RUN  --mount=type=cache,target=/root/.cache/pip \
     pip install -r requirements.txt \
     && pip install -r requirements_streamlit.txt

COPY streamlit streamlit
# shared and plugins are directories that contain modules that are required by the streamlit app
COPY plugins streamlit/plugins
COPY shared streamlit/shared

# cf. README.md:A note on importing
ENV PYTHONPATH "${PYTHONPATH}:/app/streamlit/plugins"
ENV PYTHONPATH "${PYTHONPATH}:/app/streamlit/shared"

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
