WITH trip_counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('stg_yellow_taxi_trips') }}) AS staging_count,
        (SELECT COALESCE(SUM(total_trips), 0)
         FROM {{ ref('fct_daily_taxi_metrics') }}) AS fact_count
)
SELECT *
FROM trip_counts
WHERE staging_count <> fact_count
