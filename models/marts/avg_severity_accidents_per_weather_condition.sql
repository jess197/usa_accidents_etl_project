{{ config(materialized='table') }}
with avg_severity_accidents_per_weather_condition as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
SELECT 
  weather_condition,
  AVG(accident_severity) AS average_severity, 
  case when average_severity >= 2.5 then 'Significant Impact on Traffic' 
       when average_severity <= 2.5 then 'Least Impart on Traffic'
   end AS severity
FROM
  avg_severity_accidents_per_weather_condition
GROUP BY
  weather_condition
ORDER BY
  average_severity desc;