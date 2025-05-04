-- models/dim_airline.sql

SELECT DISTINCT
    airline_name AS airline_id,  -- 'airline_name' is used as the unique identifier
    airline_name AS airline_name
FROM
    `data-management2-458610.flight_data.live_flight_cleaned`