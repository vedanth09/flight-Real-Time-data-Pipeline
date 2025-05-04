WITH flight_status_analysis AS (
    SELECT
        airline_id,
        airport_code AS airport_id,  -- Use airport_code instead of airport_id
        flight_status,
        COUNT(DISTINCT flight_id) AS total_flights,  -- Count distinct flight_id
        AVG(departure_delay) AS avg_departure_delay,
        AVG(arrival_delay) AS avg_arrival_delay
    FROM {{ ref('merge_flight_data') }}  -- Use ref() for proper dependency tracking
    GROUP BY flight_status, airline_id, airport_code  -- Group by airport_code
)
SELECT
    airline_id,
    airport_id,  -- Using airport_id (alias for airport_code)
    flight_status,
    total_flights,
    avg_departure_delay,
    avg_arrival_delay
FROM flight_status_analysis
ORDER BY total_flights DESC