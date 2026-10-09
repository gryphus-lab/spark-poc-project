FROM apache/spark:4.0.4

WORKDIR /opt/spark/project

# Copy application code
COPY ./src src
COPY ./apps apps
COPY requirements-runtime.txt .

USER root

# Resolve + cache Iceberg and Hadoop S3A packages into the image.
# Spark 4.0 ships Scala 2.13 and is pre-built against Hadoop 3.4, which uses the
# AWS SDK v2 bundle (the v1 aws-java-sdk-bundle is EOL and no longer used).
RUN /opt/spark/bin/spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.29.52,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 \
            --version > /dev/null && \
    if [ -s requirements-runtime.txt ]; then \
      pip install --no-cache-dir -r requirements-runtime.txt || exit 1; \
    fi && \
    chown -R 185:185 /opt/spark/project || exit 1

# Runtime environment
ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 185
