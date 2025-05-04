-- models/dim_airport.sql

SELECT DISTINCT
    departure_airport AS airport_name,
    departure_iata AS airport_iata,
    departure_icao AS airport_icao,
    arrival_airport AS arrival_airport_name,
    arrival_iata AS arrival_airport_iata,
    arrival_icao AS arrival_airport_icao
FROM
    `data-management2-458610.flight_data.live_flight_cleaned`