# Spark POC Project

A small PySpark proof-of-concept project demonstrating text transformation logic and test coverage with `pytest`.

## Architecture

This project implements a bronze→silver→gold ETL pipeline using Apache Iceberg for data lakehouse capabilities. The pipeline includes:

- **Bronze layer**: Raw data ingestion
- **Silver layer**: Data cleansing and upsert operations (see `apps/silver_upsert.py`)
- **Gold layer**: Aggregated and analytics-ready datasets

The Iceberg jobs under `apps/` handle incremental data processing with ACID guarantees and schema evolution support.

## Project Structure

- `apps/main_job.py` - main job entrypoint for Spark processing
- `apps/silver_upsert.py` - silver layer upsert job for Iceberg tables
- `src/transformations.py` - Spark DataFrame transformation helpers
- `src/utils.py` - Spark utility helpers
- `tests/` - pytest test suite
- `requirements.txt` - Python dependencies
- `mise.toml` - local development tooling and task definitions

## Prerequisites

- Python 3.11
- Java (OpenJDK 17)
- `mise` for managing the local environment
- Docker and Docker Compose for local development

## Setup

1. Install dependencies:

   ```bash
   mise run bootstrap
   ```

2. Activate the local virtual environment if not already active:

   ```bash
   source .venv/bin/activate
   ```

## Running Locally with Docker

The project provides a complete Docker Compose setup for running the Spark + Iceberg + MinIO stack locally.

### Services

The `docker-compose.yml` spins up the following services:

- **spark-master**: Spark master node (Web UI on port 8080)
- **spark-worker**: Spark worker node
- **spark-submit**: Container for submitting Spark jobs with project code mounted
- **minio**: S3-compatible object storage (Console on port 9001, API on port 9000)
- **minio-setup**: One-time setup container to create the `warehouse` bucket
- **catalog**: Iceberg REST catalog service (port 8181)

### Starting the Stack

Use the provided mise task to start all services:

```bash
mise run docker-compose-up
```

Or use Docker Compose directly:

```bash
docker compose up
```

This will start all services in the foreground. Use `docker compose up -d` to run in detached mode.

### Stopping the Stack

```bash
docker compose down
```

## Iceberg / MinIO Configuration

### Endpoints

- **MinIO Console**: http://localhost:9001
  - Username: `admin`
  - Password: `password`
- **S3 URI**: `s3a://warehouse/`
- **Iceberg REST Catalog**: http://localhost:8181

### Spark and Iceberg Versions

- **Spark**: 4.0.1
- **Iceberg Spark Runtime**: `iceberg-spark-runtime-4.0_2.13:1.10.1`
- **Iceberg AWS Bundle**: `iceberg-aws-bundle:1.10.1`
- **Iceberg REST Catalog**: `tabulario/iceberg-rest:1.10.1`

These version pins ensure compatibility between the Spark runtime, Iceberg client libraries, and the REST catalog server.

## Running Tests

Run the pytest suite:

```bash
mise run test
```

## Coverage

Generate a coverage report with:

```bash
mise run coverage
```

This writes a terminal coverage summary and `coverage.xml`.

## Notes

- The test suite currently includes coverage for both `src/transformations.py` and `src/utils.py`.
- The project uses `pytest-cov` to collect coverage data.
