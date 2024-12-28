# Sử dụng Python 3.8 làm base image
FROM python:3.8-slim-bullseye

# Đặt thư mục làm việc
WORKDIR /app

# Cài đặt các dependencies hệ thống
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Đặt biến môi trường Java
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

COPY /jars ./jars

# COPY /scripts/flink_processing.py .

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Đặt đường dẫn Flink
ENV PATH="/opt/flink/bin:${PATH}"

# Chạy ứng dụng
# CMD ["python", "flink_processing.py"]