import json
import os

from pyflink.common import WatermarkStrategy 
from pyflink.common.serialization import SimpleStringSchema 
from pyflink.common.typeinfo import Types 
from pyflink.datastream import StreamExecutionEnvironment 
from pyflink.datastream.connectors.kafka import ( 
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
    KafkaSource)
from pyflink.datastream import RuntimeExecutionMode

JARS_PATH = f"{os.getcwd()}/jars"

def merge_features(record):
    try:
        record_dict = json.loads(record)
        
        # print("Full record structure:", json.dumps(record_dict, indent=2))
        
        payload = record_dict.get('payload', {}).get('after', {})
        
        required_fields = [
            'vendor_id', 
            'tpep_pickup_datetime', 
            'tpep_dropoff_datetime', 
            'passenger_count', 
            'trip_distance',
            'ratecode_id', 
            'store_and_fwd_flag', 
            'pu_location_id', 
            'do_location_id', 
            'payment_type', 
            'fare_amount', 
            'total_amount' 
        ]
        
        for field in required_fields:
            if field not in payload:
                print(f"Missing required field: {field}")
                return None

        # return json.dumps(payload)
        return record
    
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        # print("Problematic record:", record)
        return None
    except Exception as e:
        print(f"Unexpected error processing record: {e}")
        # print("Problematic record:", record)
        return None


def filter_small_features(record):
    """
    Skip records containing a feature that is smaller than 0.5.
    """
    print("Raw record structure:", record)
    try:
        record_dict = json.loads(record)
        payload = record_dict.get('payload', {}).get('after', {})
        
        return payload.get('trip_distance', 0) > 0.5
    except Exception as e:
        print(f"Error parsing record: {e}")
    return True


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # The other commented lines are for Avro format
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # Define the source to take data from
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29092")
        .set_topics("mysql.nyc-taxi.taxi_trip_records")
        .set_group_id("consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define the sink to save the processed data to
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("broker:29092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("postgres.taxi_trip_records")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # No sink, just print out to the terminal
    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
    #     filter_small_features
    # ).map(merge_features).print()

    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").map(merge_features).print()

    # Add a sink to be more industrial, remember to cast to STRING in map
    # it will not work if you don't do it
    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
    #     filter_small_features
    # ).map(merge_features, output_type=Types.STRING()).sink_to(sink=sink)

    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")\
        .map(merge_features, output_type=Types.STRING()).sink_to(sink=sink)

    # Execute the job
    env.execute("flink_datastream_demo")
    print("Your job has been started successfully!")


if __name__ == "__main__":
    main()
