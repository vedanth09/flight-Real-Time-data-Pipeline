-- models/dim_flight.sql

SELECT DISTINCT
    flight_iata,
    flight_icao,
    flight_number,
    flight_status,
    flight_date
FROM
    `data-management2-458610.flight_data.live_flight_cleaned`