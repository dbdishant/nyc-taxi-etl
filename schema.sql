
CREATE TABLE IF NOT EXISTS taxi_trip_data(
        id BIGSERIAL PRIMARY KEY,
        vendorID int, 
        pickup_datetime TIMESTAMP NOT NULL, 
        dropoff_datetime TIMESTAMP NOT NULL,
        passenger_count INTEGER, 
        trip_distance DOUBLE PRECISION, 
        RatecodeID INTEGER, 
        store_and_fwd_flag VARCHAR(1),
        PULocationID INTEGER, 
        DOLocationID INTEGER, 
        payment_type INTEGER, 
        fare_amount DECIMAL(12,2), 
        extra DECIMAL(12,2),
        mta_tax DECIMAL(12,2), 
        tip_amount DECIMAL(12,2), 
        tolls_amount DECIMAL(12,2), 
        improvement_surcharge DECIMAL(12,2),
        total_amount DECIMAL(12,2), 
        congestion_surcharge DECIMAL(12,2), 
        Airport_fee DECIMAL(12,2),
        cbd_congestion_fee DECIMAL(12,2),

        CONSTRAINT valid_trip_distance
            CHECK (trip_distance >= 0),

        CONSTRAINT valid_passenger_count
            CHECK (passenger_count >= 0),

        CONSTRAINT valid_trip_duration
            CHECK (dropoff_datetime >= pickup_datetime)           
);
