FROM apache/spark:3.5.0

WORKDIR /opt/spark/project

COPY requirements-runtime.txt .

USER root

RUN mkdir -p /root/.ivy2 && \
    if [ -s requirements-runtime.txt ]; then pip install --no-cache-dir -r requirements-runtime.txt; fi && \
    /opt/spark/bin/spark-shell --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 -i /dev/null && \
    cp /root/.ivy2/jars/* /opt/spark/jars/ && \
    rm -rf /root/.ivy2

COPY . .
RUN chown -R 185:185 /opt/spark/project

ENV PYTHONPATH="/opt/spark/python:/opt/spark/project/src"
ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"

USER 185
