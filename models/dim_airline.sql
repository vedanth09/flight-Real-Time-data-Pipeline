-- models/dim_airline.sql

SELECT DISTINCT
    airline_iata,
    airline_icao,
    airline_name
FROM
    `data-management2-458610.flight_data.live_flight_cleaned`