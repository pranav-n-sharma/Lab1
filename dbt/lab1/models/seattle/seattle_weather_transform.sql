SELECT
    DATE_TRUNC('week', record_date) AS week_start_date,
    latitude,
    longitude,
    AVG(rain_sum_mm) AS avg_weekly_rain_mm,
    COUNT(record_date) AS total_days_recorded
FROM {{ source('raw', 'seattle_weather_data') }}
GROUP BY 
    DATE_TRUNC('week', record_date),
    latitude,
    longitude
ORDER BY week_start_date DESC