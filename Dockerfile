# Use a stable, verified version
FROM apache/spark:3.5.0

WORKDIR /opt/spark/project

# 1. Prepare for dependency installation
COPY requirements.txt .

USER root

# 2. Install Python deps and necessary tools for jar management
RUN pip install --no-cache-dir -r requirements.txt

# 3. Pre-download S3/Hadoop Jars
# We use the specific versions compatible with Spark 3.5.0 (Hadoop 3.3.4)
# This prevents the container from needing to download them every time it starts
RUN /opt/spark/bin/spark-shell --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --version && \
    cp /root/.ivy2/jars/* /opt/spark/jars/ && \
    rm -rf /root/.ivy2

# 4. Copy source and fix permissions in one layer
COPY . .
RUN chown -R 185:185 /opt/spark/project

# 5. Environment Config
ENV PYTHONPATH="/opt/spark/project/src"
# Force Spark to use the jars we just downloaded
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"

USER 185
