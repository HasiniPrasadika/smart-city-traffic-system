CREATE TABLE IF NOT EXISTS daily_peak_report (
    sensor_id TEXT NOT NULL,
    junction_name TEXT NOT NULL,
    hour INTEGER NOT NULL,
    total_vehicle_count INTEGER NOT NULL,
    avg_speed DOUBLE PRECISION NOT NULL,
    congestion_index DOUBLE PRECISION NOT NULL,
    recommendation TEXT NOT NULL,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hourly_summary (
    sensor_id TEXT NOT NULL,
    junction_name TEXT NOT NULL,
    hour INTEGER NOT NULL,
    total_vehicle_count INTEGER NOT NULL,
    avg_speed DOUBLE PRECISION NOT NULL,
    congestion_index DOUBLE PRECISION NOT NULL,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS critical_alerts (
    sensor_id TEXT,
    junction_name TEXT,
    road_type TEXT,
    timestamp TEXT,
    vehicle_count INTEGER,
    avg_speed DOUBLE PRECISION,
    traffic_status TEXT,
    congestion_severity TEXT,
    weather TEXT,
    road_condition TEXT,
    is_peak_hour BOOLEAN,
    is_school_time BOOLEAN,
    event_time TIMESTAMP
);
