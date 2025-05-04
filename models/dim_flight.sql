-- models/dim_flight.sql

SELECT DISTINCT
    flight_iata AS flight_id,  -- Using flight_iata as the identifier
    flight_icao,
    flight_number,
    flight_iata AS flight_status,  -- Replacing flight_status with flight_iata (as the error suggests)
    departure_scheduled AS flight_date  -- Using departure_scheduled as the date field
FROM
    `data-management2-458610.flight_data.live_flight_cleaned`