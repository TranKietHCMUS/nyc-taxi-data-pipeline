{{ config(
    materialized='table'
) }}

WITH vendor_revenue AS (
    SELECT 
        vendor_id,
        COUNT(*) as total_trips,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_trip_revenue,
        SUM(fare_amount) as total_fare_amount,
        SUM(tip_amount) as total_tips,
        SUM(tolls_amount) as total_tolls,
        SUM(airport_fee) as total_airport_fees
    FROM {{ source('raw', 'taxi_trip_records') }}
    GROUP BY vendor_id
)

SELECT * FROM vendor_revenue