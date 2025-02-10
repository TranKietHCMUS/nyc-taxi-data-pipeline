import datetime
import joblib
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy import CheckConstraint

app = Flask(__name__)

# connect to mysql
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:k6@localhost:3307/nyc-taxi'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Load the ML model using joblib
try:
    with open('model/nyc-taxi-prediction-model.joblib', 'rb') as file:
        model = joblib.load(file)
except Exception as e:
    print(f"Error loading model: {e}")
    model = None


class TaxiTripRecord(db.Model):
    __tablename__ = 'taxi_trip_records'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    VendorID = db.Column(db.Integer, nullable=False)
    tpep_pickup_datetime = db.Column(db.DateTime, nullable=False)
    tpep_dropoff_datetime = db.Column(db.DateTime, nullable=False)

    passenger_count = db.Column(db.Integer, 
        CheckConstraint('passenger_count >= 0'), 
        nullable=False
    )
    trip_distance = db.Column(db.Float, 
        CheckConstraint('trip_distance >= 0'), 
        nullable=False
    )
    RatecodeID = db.Column(db.Integer, nullable=False)
    PULocationID = db.Column(db.Integer, nullable=False)
    DOLocationID = db.Column(db.Integer, nullable=False)
    payment_type = db.Column(db.Integer, nullable=False)
    fare_amount = db.Column(db.Float, 
        CheckConstraint('fare_amount >= 0'), 
        nullable=False
    )
    extra = db.Column(db.Float, default=0.0, nullable=False)
    mta_tax = db.Column(db.Float, default=0.0, nullable=False)
    tip_amount = db.Column(db.Float, default=0.0, nullable=False)
    tolls_amount = db.Column(db.Float, default=0.0, nullable=False)
    improvement_surcharge = db.Column(db.Float, default=0.0, nullable=False)
    total_amount = db.Column(db.Float, 
        CheckConstraint('total_amount >= 0'), 
        nullable=False
    )
    congestion_surcharge = db.Column(db.Float, default=0.0, nullable=False)
    airport_fee = db.Column(db.Float, default=0.0, nullable=False)

    def to_dict(self):
        return {
            'VendorID': self.VendorID,
            'tpep_pickup_datetime': self.tpep_pickup_datetime.isoformat() if self.tpep_pickup_datetime else None,
            'tpep_dropoff_datetime': self.tpep_dropoff_datetime.isoformat() if self.tpep_dropoff_datetime else None,
            'passenger_count': self.passenger_count,
            'trip_distance': self.trip_distance,
            'RatecodeID': self.RatecodeID,
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
        return cls(
            VendorID=data.get('VendorID'),
            tpep_pickup_datetime=datetime.datetime.fromisoformat(data.get('tpep_pickup_datetime')) if data.get('tpep_pickup_datetime') else None,
            tpep_dropoff_datetime=datetime.datetime.fromisoformat(data.get('tpep_dropoff_datetime')) if data.get('tpep_dropoff_datetime') else None,
            passenger_count=data.get('passenger_count'),
            trip_distance=data.get('trip_distance'),
            RatecodeID=data.get('RatecodeID'),
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


@app.route("/api/data", methods=["POST"])
def data():
    if not request.json:
        return jsonify({"message": "No input data"}), 400

    try:
        record = TaxiTripRecord.create_from_dict(request.json)
        
        db.session.add(record)
        db.session.commit()
        
        return jsonify({
            "message": "Success", 
            "record_id": f"{record.VendorID}_{record.tpep_pickup_datetime}"
        }), 201

    except ValueError as ve:
        return jsonify({"message": f"Invalid data: {str(ve)}"}), 400
    except IntegrityError as ie:
        db.session.rollback()
        return jsonify({"message": f"Database integrity error: {str(ie)}"}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({"message": f"Unexpected error: {str(e)}"}), 500
    

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5000)