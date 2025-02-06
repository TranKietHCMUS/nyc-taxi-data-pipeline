{{ config(
    materialized='table'
) }}

WITH trip_duration AS (
    SELECT 
        id,
        vendor_id,
        -- Chuyển đổi string datetime thành timestamp và tính thời gian di chuyển bằng giây
        EXTRACT(EPOCH FROM (
            TO_TIMESTAMP(tpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') - 
            TO_TIMESTAMP(tpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS')
        )) as duration_seconds,
        trip_distance,
        pu_location_id,
        do_location_id
    FROM {{ source('raw', 'taxi_trip_records') }}
)

SELECT * FROM trip_duration