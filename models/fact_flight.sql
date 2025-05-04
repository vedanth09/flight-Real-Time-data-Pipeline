-- models/fact_flight.sql

WITH enriched_flight_data AS (
    SELECT
        lf.flight_iata,
        lf.flight_icao,
        lf.flight_number,
        lf.airline_name,
        sf.latitude_deg AS airport_latitude,
        sf.longitude_deg AS airport_longitude,
        sf.elevation_ft AS airport_elevation,
        lf.departure_airport,
        lf.arrival_airport,
        CAST(lf.departure_delay AS INT64) AS departure_delay,
        CAST(lf.arrival_delay AS INT64) AS arrival_delay,
        CAST(lf.live_speed_horizontal AS FLOAT64) AS live_speed_horizontal,
        CAST(lf.live_speed_vertical AS FLOAT64) AS live_speed_vertical,
        CAST(lf.live_altitude AS INT64) AS live_altitude,
        lf.live_is_ground,
        lf.live_direction,
        lf.live_updated,
        lf.ingest_datetime
    FROM
        `data-management2-458610.flight_data.live_flight_cleaned` AS lf
    LEFT JOIN
        `data-management2-458610.flight_data.static_flight_data` AS sf
    ON
        lf.departure_airport = sf.name
)
SELECT * FROM enriched_flight_data