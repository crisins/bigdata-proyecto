CREATE TABLE IF NOT EXISTS control_ejecucion (
    id SERIAL PRIMARY KEY,
    proceso VARCHAR(120) NOT NULL,         
    tipo VARCHAR(20) NOT NULL,              
    fecha_ejecucion TIMESTAMPTZ NOT NULL,
    estado VARCHAR(20) NOT NULL,            
    registros_leidos INTEGER DEFAULT 0,
    registros_cargados INTEGER DEFAULT 0,
    registros_rechazados INTEGER DEFAULT 0,
    registros_duplicados INTEGER DEFAULT 0,
    detalle TEXT
);


CREATE TABLE IF NOT EXISTS taxi_trips (
    id SERIAL PRIMARY KEY,
    trip_key VARCHAR(64) UNIQUE NOT NULL,   
    vendor_id INTEGER,
    pickup_datetime TIMESTAMPTZ,
    dropoff_datetime TIMESTAMPTZ,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    trip_duration_min DOUBLE PRECISION,      
    pickup_hour INTEGER,                    
    pickup_dow INTEGER,                     
    load_ts TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS component_prices (
    id SERIAL PRIMARY KEY,
    event_key VARCHAR(64) UNIQUE NOT NULL,
    component_id VARCHAR(60),
    component_name VARCHAR(200),
    price DOUBLE PRECISION,
    currency VARCHAR(10),
    cantidad INTEGER,
    monto DOUBLE PRECISION,
    forma_pago VARCHAR(50),
    cliente VARCHAR(200),
    event_timestamp TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    load_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_taxi_pu_hour ON taxi_trips (pu_location_id, pickup_hour);
CREATE INDEX IF NOT EXISTS idx_taxi_route ON taxi_trips (pu_location_id, do_location_id);
CREATE INDEX IF NOT EXISTS idx_component_time ON component_prices (component_id, event_timestamp);
