FROM apache/spark:3.5.0 AS builder

WORKDIR /opt/spark/project

# Prepare dependencies and download Spark packages early (cache layer)
COPY requirements-runtime.txt .

USER root

RUN mkdir -p /tmp/ivy && \
    if [ -s requirements-runtime.txt ]; then \
      pip install --no-cache-dir -r requirements-runtime.txt; \
    fi && \
    echo "pass" > /tmp/noop.py && \
    /opt/spark/bin/spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1 \
      --conf spark.jars.ivy=/tmp/ivy \
      /tmp/noop.py && \
    test -d /tmp/ivy/jars && \
    test -n "$(ls -A /tmp/ivy/jars)" && \
    cp /tmp/ivy/jars/* /opt/spark/jars/ && \
    rm -rf /tmp/ivy

# Final stage: minimal runtime image
FROM apache/spark:3.5.0

WORKDIR /opt/spark/project

# Copy pre-downloaded jars from builder
COPY --from=builder /opt/spark/jars /opt/spark/jars

# Copy runtime requirements and install in final image
COPY requirements-runtime.txt .

# Copy application code
COPY ./src src
COPY ./apps apps

USER root
RUN if [ -s requirements-runtime.txt ]; then \
      pip install --no-cache-dir -r requirements-runtime.txt; \
    fi && \
    chown -R 185:185 /opt/spark/project && \
    chown -R 185:185 /opt/spark/jars && \
    find /usr/local/lib -type d -name site-packages -exec chown -R 185:185 {} + 2>/dev/null || true

# Runtime environment
ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 185
