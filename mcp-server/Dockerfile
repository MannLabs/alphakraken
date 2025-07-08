FROM python:3.11-slim
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Install Python dependencies
COPY mcp-server/requirements_mcp.txt .
COPY shared/requirements_shared.txt .
RUN pip install --upgrade pip && pip install -r requirements_mcp.txt && pip install -r requirements_shared.txt

COPY mcp-server/server.py .
COPY shared shared

EXPOSE 8089

CMD ["python", "/app/server.py"]

# Create a non-root user
RUN useradd -m appuser
USER appuser

# build with
# docker build -t mcpserver -f mcp-server/Dockerfile .
# run
# docker run -t mcpserver

# TODO logs
