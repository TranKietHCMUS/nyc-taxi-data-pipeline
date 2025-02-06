import mysql.connector
import random
from datetime import datetime, timedelta
import time

# Kết nối đến MySQL database
mydb = mysql.connector.connect(
  host="mysql",
  port="3307",
  user="root",
  password="k6",
  database="nyc-taxi"
)

cursor = mydb.cursor()

# Hàm tạo dữ liệu ngẫu nhiên
def generate_random_data():
  vendor_id = random.randint(1, 5)  # Giả sử có 5 VendorID
  pickup_datetime = datetime.now() - timedelta(days=random.randint(0, 365))
  dropoff_datetime = pickup_datetime + timedelta(minutes=random.randint(1, 60))
  passenger_count = random.randint(1, 4)
  trip_distance = round(random.uniform(0.5, 10.0), 2)
  rate_code_id = random.randint(1, 6)  # Giả sử có 6 RatecodeID
  pu_location_id = random.randint(1, 263)  # Giả sử có 263 PULocationID
  do_location_id = random.randint(1, 263)  # Giả sử có 263 DOLocationID
  payment_type = random.randint(1, 6)  # Giả sử có 6 payment_type
  fare_amount = round(random.uniform(3.0, 50.0), 2)
  extra = round(random.uniform(0.0, 5.0), 2)
  mta_tax = 0.5 if random.random() > 0.5 else 0.0
  tip_amount = round(random.uniform(0.0, 10.0), 2)
  tolls_amount = round(random.uniform(0.0, 5.0), 2)
  improvement_surcharge = 0.3
  total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge
  congestion_surcharge = round(random.uniform(0.0, 2.5), 2)
  airport_fee = 1.25 if random.random() > 0.8 else 0.0

  return (vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, rate_code_id, 
          pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, 
          tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)

# Tạo và chèn dữ liệu vào bảng
def insert_data(data):
  sql = """INSERT INTO taxi_trip_records (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, 
  trip_distance, rate_code_id, pu_location_id, do_location_id, payment_type, fare_amount, extra, 
  mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee) 
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
  cursor.execute(sql, data)
  mydb.commit()

# Tạo 1000 bản ghi
for i in range(1000):
  data = generate_random_data()
  insert_data(data)
  print(f"Inserted record {i+1}")
  
  time.sleep(3)

# Đóng kết nối
cursor.close()
mydb.close()