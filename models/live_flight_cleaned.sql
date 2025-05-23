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
        `data-management2-458610.flight_data.live_flight_cleaned`  -- Ensure this dataset and table exist
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
        
        -- Handle "N/A" values and cast to FLOAT64
        CAST(CASE WHEN live_latitude = 'N/A' THEN NULL ELSE live_latitude END AS FLOAT64) AS live_latitude,
        CAST(CASE WHEN live_longitude = 'N/A' THEN NULL ELSE live_longitude END AS FLOAT64) AS live_longitude,
        CAST(CASE WHEN live_speed_horizontal = 'N/A' THEN NULL ELSE live_speed_horizontal END AS FLOAT64) AS live_speed_horizontal,
        CAST(CASE WHEN live_speed_vertical = 'N/A' THEN NULL ELSE live_speed_vertical END AS FLOAT64) AS live_speed_vertical,
        
        -- CAST live_altitude to FLOAT64 to handle decimal values like 335.28
        CAST(CASE WHEN live_altitude = 'N/A' THEN NULL ELSE live_altitude END AS FLOAT64) AS live_altitude,
        
        live_is_ground,
        live_direction,
        live_updated,
        ingest_datetime
    FROM raw_flight_data
)
SELECT * FROM cleaned_flight_data