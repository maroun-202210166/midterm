FROM python:3.10-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn requests psycopg2-binary
COPY storage_node ./storage_node
COPY metadata_service ./metadata_service