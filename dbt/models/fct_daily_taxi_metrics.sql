{{ config(materialized='table') }}

SELECT
    pickup_datetime::date AS trip_date,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance_miles,
    ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0)::numeric, 2)
        AS avg_duration_minutes,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_trip_amount
FROM {{ ref('stg_yellow_taxi_trips') }}
GROUP BY pickup_datetime::date
