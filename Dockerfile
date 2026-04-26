FROM apache/spark:3.5.0 AS builder

WORKDIR /opt/spark/project

# Prepare dependencies and download Spark packages early (cache layer)
COPY requirements-runtime.txt .

USER root

RUN mkdir -p /tmp/ivy && \
    if [ -s requirements-runtime.txt ]; then \
      pip install --no-cache-dir -r requirements-runtime.txt; \
    fi && \
    /opt/spark/bin/spark-shell \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1 \
      --conf spark.jars.ivy=/tmp/ivy \
      -c 'exit(0)' 2>&1 || true && \
    if [ -d /tmp/ivy/jars ]; then \
      cp /tmp/ivy/jars/* /opt/spark/jars/ || true; \
    fi && \
    rm -rf /tmp/ivy

# Final stage: minimal runtime image
FROM apache/spark:3.5.0

WORKDIR /opt/spark/project

# Copy pre-downloaded jars and dependencies from builder
COPY --from=builder /opt/spark/jars /opt/spark/jars
COPY --from=builder /usr/local/lib/python*/dist-packages /usr/local/lib/python*/dist-packages

# Copy application code
COPY . .

USER root
RUN chown -R 185:185 /opt/spark/project

# Runtime environment
ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 185
