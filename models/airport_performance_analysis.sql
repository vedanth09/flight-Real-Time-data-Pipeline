WITH airport_performance AS (
    SELECT
        airport_code AS airport_id,  -- Adjusting to the correct column name (airport_code)
        AVG(departure_delay) AS avg_departure_delay,
        AVG(arrival_delay) AS avg_arrival_delay,
        COUNT(*) AS total_flights
    FROM {{ ref('merge_flight_data') }}  -- Reference the merge_flight_data model
    GROUP BY airport_code  -- Group by the correct airport identifier
)
SELECT
    a.airport_id,
    a.avg_departure_delay,
    a.avg_arrival_delay,
    a.total_flights,
    s.name AS airport_name,  -- Correct column for airport name
    s.iso_country AS country  -- Correct column for country (iso_country)
FROM airport_performance a
JOIN {{ ref('static_flight_data') }} s
    ON a.airport_id = s.gps_code  -- Joining with gps_code from static_flight_data
ORDER BY a.avg_departure_delay DESC  -- Order by average departure delay
