-- Inspect selected 2026 months after running the pipeline.
SELECT DATE_TRUNC('month', trip_date)::date AS month,
       SUM(total_trips) AS clean_trips,
       ROUND(SUM(total_revenue)::numeric, 2) AS total_amount_usd
FROM dbt_dev.fct_daily_taxi_metrics
GROUP BY 1
ORDER BY 1;
