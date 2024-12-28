FROM python:3.8-slim-buster

# Install Java and necessary dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl

# Set Java environment variables
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# Create directory for additional jars
RUN mkdir -p /opt/spark/jars

# Download Hadoop and S3A dependencies
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar -O /opt/spark/jars/aws-java-sdk-bundle-1.12.261.jar

COPY jars/postgresql-42.5.0.jar /opt/spark/jars/postgresql-42.5.0.jar

# install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV S3_ENDPOINT="http://datalake-minio:9000"
ENV S3_ACCESS_KEY="minio_access_key"
ENV S3_SECRET_KEY="minio_secret_key"
ENV S3_BUCKET="taxi-time-series"
ENV FOLDER_NAME="yellow_tripdata"

ENV POSTGRES_URL="jdbc:postgresql://data-warehouse:5432/nyc-taxi"
ENV POSTGRES_USER="k6"
ENV POSTGRES_PASSWORD="k6"
ENV POSTGRES_TABLE="taxi_trip_records"

# Set Spark configuration
ENV SPARK_OPTS="--packages io.delta:delta-core_2.12:2.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Set working directory
WORKDIR /app

# Copy your script
COPY /jobs/minio2postgres.py .

CMD ["python", "minio2postgres.py"]