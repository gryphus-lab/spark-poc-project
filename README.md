# Spark POC Project

A PySpark proof-of-concept project that demonstrates a bronze→silver→gold ETL pipeline with Apache Iceberg tables and MinIO-backed object storage.

## Latest Updates

- Spark/PySpark is pinned to **3.5.8** across Docker and Python dependencies.
- Iceberg dependencies remain on the Spark 3.5-compatible line (`1.10.1` artifacts).
- The Docker Compose stack includes Spark + MinIO services (no Iceberg REST catalog container).
- Setup and run instructions are aligned with current `mise` tasks and environment files.

## Architecture

This project implements a bronze→silver→gold ETL flow:

- **Bronze layer**: Raw data ingestion
- **Silver layer**: Data cleansing and upsert operations (`apps/silver_upsert.py`)
- **Gold layer**: Aggregated, analytics-ready datasets

The pipeline uses an Iceberg Hadoop catalog named `local` with warehouse path `/opt/spark/warehouse`.

## Project Structure

- `apps/main_job.py` - main ETL job entrypoint
- `apps/silver_upsert.py` - silver-layer Iceberg upsert job
- `src/transformations.py` - DataFrame transformation and merge helpers
- `src/utils.py` - Spark session and catalog configuration
- `tests/` - pytest test suite (logic, integration, quality, and Docker config checks)
- `requirements.txt` - Python runtime and development dependencies
- `pyproject.toml` - project metadata and pinned PySpark dependency
- `mise.toml` - local tooling and task definitions
- `docker-compose.yml` / `Dockerfile` - local Spark + MinIO stack

## Prerequisites

- Python 3.11
- Java 17 (OpenJDK/Temurin)
- `mise`
- Docker and Docker Compose

## Setup

1. Create your local environment file:

   ```bash
   cp .env.example .env
   ```

2. Install dependencies:

   ```bash
   mise run bootstrap
   ```

3. Activate the virtual environment if needed:

   ```bash
   source .venv/bin/activate
   ```

4. Add input data (CSV with columns `id,name,status`) to:

   - `data/input/sample.csv`

## Running Locally with Docker

### Services

The `docker-compose.yml` stack starts:

- **spark-master**: Spark master node (Web UI on port `8080`)
- **spark-worker**: Spark worker node
- **spark-submit**: Spark job container with project code mounted
- **minio**: S3-compatible object storage (API on `9000`, console on `9001`)
- **minio-setup**: one-time bucket setup for `warehouse`

### Starting the Stack

Use the `mise` task:

```bash
mise run docker-compose-up
```

Or directly:

```bash
docker compose up --build -d
```
### Running ETL Jobs

Run ETL jobs from the `spark-submit` service once the stack is up.

1. Define package coordinates used by both jobs:

```bash
SPARK_PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1"
```

2. Run the full bronze→silver→gold job:

```bash
docker compose exec spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "${SPARK_PACKAGES}" \
  /opt/spark/project/apps/main_job.py
```

3. Run the silver upsert job:

```bash
docker compose exec spark-submit /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "${SPARK_PACKAGES}" \
  /opt/spark/project/apps/silver_upsert.py
```

`apps/silver_upsert.py` reads from `s3a://warehouse/input/sample.csv` by default. Upload that object to the `warehouse` bucket first (for example via MinIO Console).

### Stopping the Stack

Use the `mise` task:

```bash
mise run docker-compose-down
```

Or directly:

```bash
docker compose down --volumes --remove-orphans --rmi all
```

## Endpoints and Storage

- **Spark Master UI**: <http://localhost:8080>
- **MinIO API**: <http://localhost:9000>
- **MinIO Console**: <http://localhost:9001>
- **S3 Warehouse URI**: `s3a://warehouse/`

Default local credentials come from `.env` (`admin` / `password` by default).

## Spark and Iceberg Versions

- **Spark image**: `apache/spark:3.5.8`
- **PySpark**: `pyspark==3.5.8`
- **Hadoop AWS**: `org.apache.hadoop:hadoop-aws:3.3.4`
- **AWS SDK bundle**: `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- **Iceberg Spark Runtime**: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1`
- **Iceberg AWS Bundle**: `org.apache.iceberg:iceberg-aws-bundle:1.10.1`

The current stack targets Spark 3.5.x to stay aligned with the Iceberg runtime used in this project.

## Running Tests

Run the test suite:

```bash
mise run test
```

Run lint checks:

```bash
mise run lint
```

## Coverage

Generate a coverage report:

```bash
mise run coverage
```

This writes a terminal coverage summary and `coverage.xml`.
