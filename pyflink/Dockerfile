# Sử dụng Python 3.9 làm base image
FROM python:3.9-slim-bullseye

# Đặt thư mục làm việc
WORKDIR /app

# Cài đặt các dependencies hệ thống
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Đặt biến môi trường Java
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

COPY /jars/flink-connector-kafka-1.17.1.jar ./jars/flink-connector-kafka-1.17.1.jar
COPY /jars/kafka-clients-3.4.0.jar ./jars/kafka-clients-3.4.0.jar

COPY /schemas/nyc-taxi-trip.json ./schemas/nyc-taxi-trip.json

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Đặt đường dẫn Flink
ENV PATH="/opt/flink/bin:${PATH}"

COPY /scripts/flink_processing.py .