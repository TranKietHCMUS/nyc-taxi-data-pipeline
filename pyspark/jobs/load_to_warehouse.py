from datetime import datetime
import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def read_parquet_file(spark, file_path):
    try:
        # read file parquet
        df = spark.read.parquet(file_path)
        return df
    except Exception as e:
        print(f"Error reading Delta table: {e}")
        traceback.print_exc()
        return None

def main():
    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    s3_bucket = os.getenv("S3_SILVER_BUCKET")
    file_name = os.getenv("FILE_NAME")

    postgres_url = os.getenv("POSTGRES_URL")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_table = os.getenv("POSTGRES_TABLE")
    
    spark = (
        SparkSession.builder
        .appName("DeltaTableToPostgres")
        .config("spark.jars.packages", 
            # "io.delta:delta-core_2.12:2.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
            "org.postgresql:postgresql:42.5.0"
        )
        .config("spark.jars", 
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.261.jar,"
            "/opt/spark/jars/postgresql-42.5.0.jar"
        )
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)

    file_path = f"s3a://{s3_bucket}/{year}/{file_name}-{month}.parquet"

    df = read_parquet_file(spark, file_path)

    # drop column __index_level_0 if exists
    if "__index_level_0__" in df.columns:
        df = df.drop(col("__index_level_0__"))

    # print df count
    print(df.count())
    # show the data
    df.printSchema()

    # count number of rows from postgres table
    old_count = spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", postgres_table) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .load().count()

    # update id column with auto-incremental value from old_count + 1 to new_count + old_count + 1
    df = df.withColumn("id", col("id") + old_count)

    df.write \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", postgres_table) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("Data successfully loaded to PostgreSQL.")

    spark.stop()

if __name__ == "__main__":
    main()