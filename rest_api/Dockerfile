FROM python:3.11-slim-bookworm
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create a non-root user
RUN useradd -m appuser

# Set the working directory
WORKDIR /app

# Install Python dependencies
COPY rest_api/requirements_rest_api.txt .
COPY shared/requirements_shared.txt .
RUN pip install --upgrade pip && pip install -r requirements_rest_api.txt && pip install -r requirements_shared.txt

COPY --chown=appuser:appuser rest_api/ rest_api
COPY --chown=appuser:appuser shared shared

# the yaml holds the display paths, cf. shared.display_paths
RUN mkdir -p /opt/airflow/envs
COPY envs/alphakraken.*.yaml /opt/airflow/envs
# avoid sensitive data in the container
RUN sed -i '/\(username\|password\)/d'  /opt/airflow/envs/alphakraken.*.yaml

USER appuser

ENV REST_API_PORT=8090
EXPOSE $REST_API_PORT

CMD ["python", "-m", "rest_api.main"]
