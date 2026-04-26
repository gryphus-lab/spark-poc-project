# Use the official Apache Spark image (verified public registry)
FROM apache/spark:4.1.1

# Set the working directory inside the container
WORKDIR /opt/spark/project

# 1. Copy only the requirements file to leverage layer caching
COPY requirements.txt .

# 2. Switch to root to install Python dependencies
USER root
RUN pip install --no-cache-dir -r requirements.txt && \
    chown -R 185:185 /opt/spark/project

# 4. Copy the rest of the project source code
COPY . .

# 5. Set environment variables for module discovery
ENV PYTHONPATH="/opt/spark/project/src"

# Switch back to the default Spark user
USER 185
