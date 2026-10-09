FROM apache/spark:4.0.4

WORKDIR /opt/spark/project

USER root

# Install uv (pinned) from the official distroless image.
COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /usr/local/bin/

# Resolve dependencies from the lockfile first (own layer, cached unless the
# lockfile changes). The project runs via PYTHONPATH rather than as an installed
# package, so only runtime deps are synced here (dev group excluded).
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/spark/project/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY ./src src
COPY ./apps apps

# Resolve + cache the Iceberg and Hadoop S3A JVM packages into the image.
# Spark 4.0 ships Scala 2.13 and is pre-built against Hadoop 3.4, which uses the
# AWS SDK v2 bundle (the v1 aws-java-sdk-bundle is EOL and no longer used).
RUN /opt/spark/bin/spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.29.52,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 \
            --version > /dev/null && \
    chown -R 185:185 /opt/spark/project

# Runtime environment. Point PySpark (driver + executors) at the uv-managed
# interpreter so pandas/numpy and friends are importable, and expose src/ on the
# path for the `from src...` imports used by the apps.
ENV VIRTUAL_ENV=/opt/spark/project/.venv \
    PATH="/opt/spark/project/.venv/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/spark/project/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/spark/project/.venv/bin/python
ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 185
