CREATE TABLE IF NOT EXISTS taxi_trip_records (
    id BIGSERIAL PRIMARY KEY, -- ID
    vendor_id INT, -- ID của nhà cung cấp
    tpep_pickup_datetime VARCHAR(50) NOT NULL, -- Ngày giờ đón khách
    tpep_dropoff_datetime VARCHAR(50) NOT NULL, -- Ngày giờ trả khách
    passenger_count INT CHECK (passenger_count >= 0), -- Số lượng hành khách (>= 0)
    trip_distance DECIMAL(10, 2) CHECK (trip_distance >= 0), -- Khoảng cách chuyến đi
    rate_code_id INT, -- Mã loại giá cước
    pu_location_id INT, -- ID địa điểm đón khách
    do_location_id INT, -- ID địa điểm trả khách
    payment_type INT, -- Phương thức thanh toán
    fare_amount DECIMAL(10, 2) CHECK (fare_amount >= 0), -- Số tiền cước
    extra DECIMAL(10, 2) DEFAULT 0.0, -- Phụ phí
    mta_tax DECIMAL(10, 2) DEFAULT 0.0, -- Thuế MTA
    tip_amount DECIMAL(10, 2) DEFAULT 0.0, -- Tiền tip
    tolls_amount DECIMAL(10, 2) DEFAULT 0.0, -- Phí cầu đường
    improvement_surcharge DECIMAL(10, 2) DEFAULT 0.0, -- Phụ phí cải tiến
    total_amount DECIMAL(10, 2) CHECK (total_amount >= 0), -- Tổng số tiền thanh toán
    congestion_surcharge DECIMAL(10, 2) DEFAULT 0.0, -- Phụ phí tắc nghẽn giao thông
    airport_fee DECIMAL(10, 2) DEFAULT 0.0, -- Phí sân bay
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SCHEMA IF NOT EXISTS visualize;