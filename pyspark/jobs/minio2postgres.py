import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

def read_delta_table(spark, delta_table_path):
    try:
        delta_df = spark.read.format("delta").load(delta_table_path)
        return delta_df
    except Exception as e:
        print(f"Error reading Delta table: {e}")
        traceback.print_exc()
        return None

def main():
    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    s3_bucket = os.getenv("S3_BUCKET")
    folder_name = os.getenv("FOLDER_NAME")

    postgres_url = os.getenv("POSTGRES_URL")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_table = os.getenv("POSTGRES_TABLE")
    
    spark = (
        SparkSession.builder
        .appName("DeltaTableToPostgres")
        .config("spark.jars.packages", 
            "io.delta:delta-core_2.12:2.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
            "org.postgresql:postgresql:42.5.0"
        )
        .config("spark.jars", 
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.261.jar,"
            "/opt/spark/jars/postgresql-42.5.0.jar"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Cấu hình S3A
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.defaultFS", f"s3a://{s3_bucket}")
        .getOrCreate()
    )

    # delta_df = None

    # for i in range(1, 10):
    #     month = str(i).zfill(2)
    #     delta_table_path = f"s3a://{s3_bucket}/{folder_name}/2024/{month}"
    #     print(delta_table_path)
    #     df = read_delta_table(spark, delta_table_path)
    #     if df:
    #         if delta_df is None:
    #             delta_df = df
    #         else:
    #             delta_df = delta_df.union(df)

    delta_table_path = f"s3a://{s3_bucket}/{folder_name}/2024/01"

    delta_df = read_delta_table(spark, delta_table_path)

    delta_df = delta_df.select(
        col("VendorID").alias("vendor_id"),
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("RatecodeID").alias("ratecode_id"),
        col("store_and_fwd_flag"),
        col("PULocationID").alias("pu_location_id"),
        col("DOLocationID").alias("do_location_id"),
        col("payment_type"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("improvement_surcharge"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("airport_fee")
    )

    cleaned_delta_df = delta_df.dropna()

    # cleaned_delta_df['store_and_fwd_flag'] = cleaned_delta_df['store_and_fwd_flag'].map({'Y': 1, 'N': 0})

    cleaned_delta_df.write \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", postgres_table) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Data successfully loaded to PostgreSQL.")

    spark.stop()

if __name__ == "__main__":
    main()
