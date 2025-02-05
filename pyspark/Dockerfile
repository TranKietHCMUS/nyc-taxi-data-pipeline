FROM python:3.10-slim-buster

# Combine RUN commands and clean up in the same layer to reduce image size
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/spark/jars

# Set environment variables in a single layer
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=$PATH:/usr/lib/jvm/java-11-openjdk-amd64/bin \
    S3_ENDPOINT="http://datalake-minio:9000" \
    S3_ACCESS_KEY="minio_access_key" \
    S3_SECRET_KEY="minio_secret_key" \
    S3_SILVER_BUCKET="silver" \
    S3_GOLDEN_BUCKET="golden" \
    FILE_NAME="yellow_tripdata" \
    POSTGRES_URL="jdbc:postgresql://data-warehouse:5432/nyc-taxi" \
    POSTGRES_USER="k6" \
    POSTGRES_PASSWORD="k6" \
    POSTGRES_TABLE="taxi_trip_records" \
    SPARK_OPTS="--packages io.delta:delta-core_2.12:2.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Download dependencies in a single layer
RUN wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar

# Copy files
COPY jars/postgresql-42.5.0.jar /opt/spark/jars/
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory and copy application code
WORKDIR /app
COPY /jobs .