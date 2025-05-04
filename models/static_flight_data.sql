-- models/static_flight_data.sql

WITH raw_static_flight_data AS (
    SELECT
        id,
        ident,
        type,
        name,
        latitude_deg,
        longitude_deg,
        elevation_ft,
        continent,
        iso_country,
        iso_region,
        municipality,
        scheduled_service,
        gps_code,
        local_code
    FROM
        `data-management2-458610.flight_data.static_flight_data`  -- Ensure this dataset and table exist
)

SELECT
    id,
    ident,
    type,
    name,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    continent,
    iso_country,
    iso_region,
    municipality,
    scheduled_service,
    gps_code,
    local_code
FROM
    raw_static_flight_data
