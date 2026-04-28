FROM apache/spark:3.5.8

WORKDIR /opt/spark/project

# Copy application code
COPY ./src src
COPY ./apps apps
COPY requirements-runtime.txt .

USER root

# Install Iceberg and Hadoop packages at runtime
RUN /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1 --version > /dev/null && \
    if [ -s requirements-runtime.txt ]; then \
      pip install --no-cache-dir -r requirements-runtime.txt || exit 1; \
    fi && \
    chown -R 185:185 /opt/spark/project || exit 1

# Runtime environment
ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 185
