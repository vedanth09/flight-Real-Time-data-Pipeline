-- models/clean_flight_data.sql
WITH enriched_flight_data AS (
    SELECT
        lf.flight_iata,
        lf.flight_icao,
        lf.flight_number,
        lf.airline_name,
        lf.arrival_airport,
        sf.latitude_deg AS airport_latitude,
        sf.longitude_deg AS airport_longitude,
        sf.elevation_ft AS airport_elevation,
        lf.departure_airport,
        lf.departure_delay,
        lf.departure_scheduled,
        lf.ingest_datetime
    FROM
        `data-management2-458610.flight_data.live_flight_cleaned` AS lf  -- Reference the live_flight_cleaned table under the correct path
    LEFT JOIN
        `data-management2-458610.flight_data.static_flight_data` AS sf  -- Reference the static_flight_data table under the correct path
    ON
        lf.departure_airport = sf.name
)
SELECT * FROM enriched_flight_data