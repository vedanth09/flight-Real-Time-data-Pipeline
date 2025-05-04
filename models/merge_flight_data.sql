WITH live_data AS (
    -- Select the necessary columns from the live flight data (cleaned)
    SELECT
        flight_iata AS flight_id,  -- Use flight_iata as the flight identifier
        airline_iata AS airline_id,
        departure_airport,
        arrival_airport,
        CAST(departure_delay AS INT64) AS departure_delay,  -- Cast to INT64
        CAST(arrival_delay AS INT64) AS arrival_delay,      -- Cast to INT64
        flight_date,
        flight_status,
        departure_scheduled AS departure_time,  -- Updated column name
        arrival_scheduled AS arrival_time,      -- Updated column name
        live_speed_horizontal,
        live_speed_vertical,
        live_altitude,
        live_is_ground
    FROM {{ source('flight_data', 'live_flight_cleaned') }}
),

static_data AS (
    -- Select the necessary columns from the static flight data (airport info)
    SELECT
        id AS airport_id,
        name AS airport_name,
        continent,
        iso_country AS country,
        iso_region AS region,
        gps_code AS airport_code  -- Ensure we are using airport_code for matching
    FROM {{ source('flight_data', 'static_flight_data') }}
)

-- Join the live data with static airport data using the departure and arrival airports
SELECT
    l.flight_id,  -- Use the flight_iata as the flight identifier
    l.airline_id,
    l.departure_airport,
    l.arrival_airport,
    l.flight_date,
    l.flight_status,
    l.departure_time,
    l.arrival_time,
    l.departure_delay,
    l.arrival_delay,
    s.airport_name,
    s.country,
    s.region,
    s.airport_code,  -- Joining with airport_code now
    l.live_speed_horizontal,
    l.live_speed_vertical,
    l.live_altitude,
    l.live_is_ground
FROM live_data l
LEFT JOIN static_data s
    ON CAST(l.departure_airport AS STRING) = CAST(s.airport_code AS STRING)  -- Corrected join with airport_code
    OR CAST(l.arrival_airport AS STRING) = CAST(s.airport_code AS STRING)    -- Similarly join for arrival airport
