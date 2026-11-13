WITH expected_months AS (
    SELECT DISTINCT month_number
    FROM UNNEST(
        ARRAY[{{ var('months', [7, 8, 9]) | join(', ') }}]::int[]
    ) AS t(month_number)
),
actual_months AS (
    SELECT DISTINCT EXTRACT(MONTH FROM pickup_datetime)::int AS month_number
    FROM {{ ref('stg_yellow_taxi_trips') }}
)
SELECT e.month_number AS missing_month
FROM expected_months e
LEFT JOIN actual_months a
  ON e.month_number = a.month_number
WHERE a.month_number IS NULL
