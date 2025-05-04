-- models/flight_delay_trends.sql

WITH airline_delay_trends AS (
    SELECT
        airline_id,  -- 'airline_id' from the merged_flight_data model
        EXTRACT(MONTH FROM CAST(flight_date AS DATE)) AS month,  -- Extract the month from the flight_date
        AVG(departure_delay) AS avg_departure_delay,
        AVG(arrival_delay) AS avg_arrival_delay
    FROM {{ ref('merge_flight_data') }}  -- Reference the merge_flight_data model
    GROUP BY airline_id, EXTRACT(MONTH FROM CAST(flight_date AS DATE))  -- Group by airline_id and month
)

SELECT
    a.airline_id,
    a.month,
    a.avg_departure_delay,
    a.avg_arrival_delay,
    b.airline_name  -- Airline name from the dimension table
FROM airline_delay_trends a
JOIN {{ ref('dim_airline') }} b  -- Joining with the dim_airline table
    ON a.airline_id = b.airline_id  -- Corrected join condition
ORDER BY a.month, a.avg_departure_delay DESC  -- Order by month and average departure delay;
