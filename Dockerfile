FROM apache/spark:3.5.0

WORKDIR /opt/spark/project

COPY requirements-runtime.txt .

USER root

RUN mkdir -p /tmp/ivy && \
    if [ -s requirements-runtime.txt ]; then pip install --no-cache-dir -r requirements-runtime.txt; fi && \
    /opt/spark/bin/spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1 \
      --conf spark.jars.ivy=/tmp/ivy \
      --class org.apache.spark.deploy.SparkSubmit /dev/null 2>&1 && \
    test -d /tmp/ivy/jars && \
    cp /tmp/ivy/jars/* /opt/spark/jars/ && \
    rm -rf /tmp/ivy

COPY . .
RUN chown -R 185:185 /opt/spark/project

ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"

USER 185
