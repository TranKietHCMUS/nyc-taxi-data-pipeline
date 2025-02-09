CREATE TABLE taxi_trip_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    vendor_id INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT CHECK (passenger_count >= 0),
    trip_distance DECIMAL(10, 2) CHECK (trip_distance >= 0),
    rate_code_id INT,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount DECIMAL(10, 2) CHECK (fare_amount >= 0),
    extra DECIMAL(10, 2) DEFAULT 0.0,
    mta_tax DECIMAL(10, 2) DEFAULT 0.0,
    tip_amount DECIMAL(10, 2) DEFAULT 0.0,
    tolls_amount DECIMAL(10, 2) DEFAULT 0.0,
    improvement_surcharge DECIMAL(10, 2) DEFAULT 0.0,
    total_amount DECIMAL(10, 2) CHECK (total_amount >= 0),
    congestion_surcharge DECIMAL(10, 2) DEFAULT 0.0,
    airport_fee DECIMAL(10, 2) DEFAULT 0.0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE predicts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    vendor_id INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT CHECK (passenger_count >= 0),
    trip_distance DECIMAL(10, 2) CHECK (trip_distance >= 0),
    pu_location_id INT,
    do_location_id INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)