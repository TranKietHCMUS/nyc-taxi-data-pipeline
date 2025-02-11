from datetime import datetime
import os
import traceback
from pyspark.sql import SparkSession

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
    s3_bucket2 = os.getenv("S3_SILVER_BUCKET")
    s3_bucket3 = os.getenv("S3_GOLDEN_BUCKET")
    file_name = os.getenv("FILE_NAME")
    
    spark = (
        SparkSession.builder
        .appName("SilverToGolden")
        .config("spark.jars.packages", 
            "io.delta:delta-core_2.12:2.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
        )
        .config("spark.jars", 
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.261.jar,"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "2")\
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+HeapDumpOnOutOfMemoryError") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
        .config("spark.sql.files.maxRecordsPerFile", "500000")\
        .config("spark.sql.files.minRecordsPerFile", "100000")\

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
        .config("spark.hadoop.fs.defaultFS", f"s3a://{s3_bucket3}")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)

    try:
        # Define paths
        source_path = f"s3a://{s3_bucket2}/{year}/{file_name}-{month}.parquet"
        delta_table_path = f"s3a://{s3_bucket3}/{file_name}"

        # Read new data
        new_df = spark.read.parquet(source_path)
        
        if new_df.count() == 0:
            print("No valid data found to merge")
            return
        
        print("Successfully read new data at " + source_path + " with count: " + str(new_df.count()))

        # select columns
        new_df.drop("vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pu_location_id", "do_location_id", "trip_distance")

        new_df = new_df.withColumn("tpep_pickup_datetime", new_df["tpep_pickup_datetime"].cast("long"))
        new_df = new_df.withColumn("tpep_dropoff_datetime", new_df["tpep_dropoff_datetime"].cast("long"))

        new_df = new_df.dropna()

        try:
            # Try to read existing Delta table
            existing_df = spark.read.format("delta").load(delta_table_path)
            print("Successfully read existing Delta Table at " + delta_table_path + " with count: " + str(existing_df.count()))

            # Merge data
            # Union existing and new data and remove duplicates
            merged_df = existing_df.union(new_df).distinct()

            # Write back to Delta table
            merged_df.coalesce(1).write \
                .format("delta") \
                .mode("overwrite") \
                .save(delta_table_path)
            
            print("Successfully merged and wrote data to Delta Table. Count: " + str(merged_df.count()))

        except Exception as e:
            if "Path does not exist" in str(e):
                print("Delta Table does not exist, creating new one")
                try:
                    # Create new Delta table with new data
                    new_df.coalesce(1).write \
                        .format("delta") \
                        .mode("overwrite") \
                        .save(delta_table_path)
                    print("Successfully created new Delta Table")
                except Exception as write_error:
                    print(f"Error creating Delta Table: {write_error}")
                    return
            else:
                print(f"Error merging data: {e}")
                return

    except Exception as e:
        print(f"Unexpected error: {e}")
        return

    spark.stop()

if __name__ == "__main__":
    main()