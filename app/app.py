import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy import CheckConstraint

app = Flask(__name__)

# connect to mysql
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:k6@localhost:3307/nyc-taxi'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# NYC Taxi Model
class TaxiTripRecord(db.Model):
    __tablename__ = 'taxi_trip_records'

    # Khóa chính ghép
    VendorID = db.Column(db.Integer, primary_key=True)
    tpep_pickup_datetime = db.Column(db.DateTime, primary_key=True, nullable=False)
    tpep_dropoff_datetime = db.Column(db.DateTime, primary_key=True, nullable=False)

    # Các cột khác
    passenger_count = db.Column(db.Integer, 
        CheckConstraint('passenger_count >= 0'), 
        nullable=True
    )
    trip_distance = db.Column(db.Float, 
        CheckConstraint('trip_distance >= 0'), 
        nullable=True
    )
    RatecodeID = db.Column(db.Integer, nullable=True)
    store_and_fwd_flag = db.Column(db.boolean, 
        CheckConstraint("store_and_fwd_flag IN ('Y', 'N')"),
        nullable=True
    )
    PULocationID = db.Column(db.Integer, nullable=True)
    DOLocationID = db.Column(db.Integer, nullable=True)
    payment_type = db.Column(db.Integer, nullable=True)
    fare_amount = db.Column(db.Float, 
        CheckConstraint('fare_amount >= 0'), 
        nullable=True
    )
    extra = db.Column(db.Float, default=0.0, nullable=True)
    mta_tax = db.Column(db.Float, default=0.0, nullable=True)
    tip_amount = db.Column(db.Float, default=0.0, nullable=True)
    tolls_amount = db.Column(db.Float, default=0.0, nullable=True)
    improvement_surcharge = db.Column(db.Float, default=0.0, nullable=True)
    total_amount = db.Column(db.Float, 
        CheckConstraint('total_amount >= 0'), 
        nullable=True
    )
    congestion_surcharge = db.Column(db.Float, default=0.0, nullable=True)
    airport_fee = db.Column(db.Float, default=0.0, nullable=True)

    def to_dict(self):
        """Chuyển đổi model thành dictionary"""
        return {
            'VendorID': self.VendorID,
            'tpep_pickup_datetime': self.tpep_pickup_datetime.isoformat() if self.tpep_pickup_datetime else None,
            'tpep_dropoff_datetime': self.tpep_dropoff_datetime.isoformat() if self.tpep_dropoff_datetime else None,
            'passenger_count': self.passenger_count,
            'trip_distance': self.trip_distance,
            'RatecodeID': self.RatecodeID,
            'store_and_fwd_flag': self.store_and_fwd_flag,
            'PULocationID': self.PULocationID,
            'DOLocationID': self.DOLocationID,
            'payment_type': self.payment_type,
            'fare_amount': self.fare_amount,
            'extra': self.extra,
            'mta_tax': self.mta_tax,
            'tip_amount': self.tip_amount,
            'tolls_amount': self.tolls_amount,
            'improvement_surcharge': self.improvement_surcharge,
            'total_amount': self.total_amount,
            'congestion_surcharge': self.congestion_surcharge,
            'airport_fee': self.airport_fee
        }

    @classmethod
    def create_from_dict(cls, data):
        """Tạo instance từ dictionary"""
        return cls(
            VendorID=data.get('VendorID'),
            tpep_pickup_datetime=datetime.datetime.fromisoformat(data.get('tpep_pickup_datetime')) if data.get('tpep_pickup_datetime') else None,
            tpep_dropoff_datetime=datetime.datetime.fromisoformat(data.get('tpep_dropoff_datetime')) if data.get('tpep_dropoff_datetime') else None,
            passenger_count=data.get('passenger_count'),
            trip_distance=data.get('trip_distance'),
            RatecodeID=data.get('RatecodeID'),
            store_and_fwd_flag=data.get('store_and_fwd_flag'),
            PULocationID=data.get('PULocationID'),
            DOLocationID=data.get('DOLocationID'),
            payment_type=data.get('payment_type'),
            fare_amount=data.get('fare_amount'),
            extra=data.get('extra', 0.0),
            mta_tax=data.get('mta_tax', 0.0),
            tip_amount=data.get('tip_amount', 0.0),
            tolls_amount=data.get('tolls_amount', 0.0),
            improvement_surcharge=data.get('improvement_surcharge', 0.0),
            total_amount=data.get('total_amount'),
            congestion_surcharge=data.get('congestion_surcharge', 0.0),
            airport_fee=data.get('airport_fee', 0.0)
        )


@app.route("/api/predict", methods=["POST"])
def predict():
    # Kiểm tra dữ liệu đầu vào
    if not request.json:
        return jsonify({"message": "No input data"}), 400

    try:
        # Tạo bản ghi từ dữ liệu JSON
        record = TaxiTripRecord.create_from_dict(request.json)
        
        # Thêm và commit bản ghi
        db.session.add(record)
        db.session.commit()
        
        return jsonify({
            "message": "Success", 
            "record_id": f"{record.VendorID}_{record.tpep_pickup_datetime}"
        }), 201

    except ValueError as ve:
        # Bắt lỗi từ việc chuyển đổi dữ liệu
        return jsonify({"message": f"Invalid data: {str(ve)}"}), 400
    except IntegrityError as ie:
        # Rollback session nếu có lỗi toàn vẹn dữ liệu
        db.session.rollback()
        return jsonify({"message": f"Database integrity error: {str(ie)}"}), 400
    except Exception as e:
        # Xử lý các lỗi không mong đợi
        db.session.rollback()
        return jsonify({"message": f"Unexpected error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5000)