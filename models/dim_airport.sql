-- models/dim_airport.sql

SELECT DISTINCT
    departure_airport AS airport_name,
    departure_actual AS airport_iata,  -- Using departure_actual as suggested
    departure_actual AS airport_icao,  -- Replacing departure_icao with departure_actual
    arrival_airport AS arrival_airport_name,
    arrival_actual AS arrival_airport_iata,  -- Replacing arrival_iata with arrival_actual
    arrival_actual AS arrival_airport_icao  -- Replacing arrival_icao with arrival_actual
FROM
    `data-management2-458610.flight_data.live_flight_cleaned`