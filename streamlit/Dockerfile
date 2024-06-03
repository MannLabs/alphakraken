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
COPY shared/db streamlit/shared/db

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
