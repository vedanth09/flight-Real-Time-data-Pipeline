-- models/live_flight_cleaned.sql

WITH raw_flight_data AS (
    SELECT
        flight_iata,
        flight_icao,
        flight_number,
        airline_name,
        arrival_airport,
        departure_airport,
        departure_delay,
        departure_scheduled,
        arrival_delay,
        arrival_scheduled,
        departure_actual,
        arrival_actual,
        live_latitude,
        live_longitude,
        live_speed_horizontal,
        live_speed_vertical,
        live_altitude,
        live_is_ground,
        live_direction,
        live_updated,
        ingest_datetime
    FROM
        {{ source('flight_data', 'live_flight_data') }}  -- Update with the correct source and table name
),
cleaned_flight_data AS (
    SELECT
        flight_iata,
        flight_icao,
        flight_number,
        airline_name,
        arrival_airport,
        departure_airport,
        CAST(departure_delay AS INT64) AS departure_delay,
        CAST(departure_scheduled AS TIMESTAMP) AS departure_scheduled,
        CAST(arrival_delay AS INT64) AS arrival_delay,
        CAST(arrival_scheduled AS TIMESTAMP) AS arrival_scheduled,
        COALESCE(departure_actual, '1970-01-01 00:00:00') AS departure_actual,
        COALESCE(arrival_actual, '1970-01-01 00:00:00') AS arrival_actual,
        CAST(live_latitude AS FLOAT64) AS live_latitude,
        CAST(live_longitude AS FLOAT64) AS live_longitude,
        CAST(live_speed_horizontal AS FLOAT64) AS live_speed_horizontal,
        CAST(live_speed_vertical AS FLOAT64) AS live_speed_vertical,
        CAST(live_altitude AS INT64) AS live_altitude,
        live_is_ground,
        live_direction,
        live_updated,
        ingest_datetime
    FROM raw_flight_data
)
SELECT * FROM cleaned_flight_data
