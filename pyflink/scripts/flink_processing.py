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

required_fields = [
    'id',
    'vendor_id', 
    'tpep_pickup_datetime', 
    'tpep_dropoff_datetime', 
    'passenger_count', 
    'trip_distance',
    'rate_code_id', 
    'pu_location_id', 
    'do_location_id', 
    'payment_type', 
    'fare_amount', 
    'total_amount'
]

def merge_features(record):
    try:
        record_dict = json.loads(record)
        
        payload = record_dict.get('payload', {}).get('after', {})

        # only get fields that are required
        payload = {k: v for k, v in payload.items() if k in required_fields}
        return json.dumps(payload)
    
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error processing record: {e}")
        return None

def filter_features(record):
    """
    Filter out records with missing values
    """
    print("Raw record structure:", record)
    try:
        record_dict = json.loads(record)
        payload = record_dict.get('payload', {}).get('after', {})
        print("Payload:", payload)
        for field in required_fields:
            if field not in payload:
                print(f"Missing field: {field}")
                return False
            if field in ['passenger_count', 'trip_distance', 'payment_type', 'fare_amount', 'total_amount']:
                if payload[field] < 0:
                    print(f"Negative value for field {field}")
                    return False
        if payload['tpep_pickup_datetime'] > payload['tpep_dropoff_datetime']:
            print("Invalid time range")
            return False
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

    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
        filter_features
    ).map(merge_features, output_type=Types.STRING()).sink_to(sink=sink)

    # Execute the job
    env.execute("flink_datastream_demo")
    print("Your job has been started successfully!")


if __name__ == "__main__":
    main()
