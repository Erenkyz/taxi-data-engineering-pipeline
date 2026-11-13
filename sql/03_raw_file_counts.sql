-- Compare cached ingestion across the four source files in the verified run.
SELECT source_file, COUNT(*) AS total_rows
FROM raw.yellow_taxi_trips
GROUP BY source_file
ORDER BY source_file;
