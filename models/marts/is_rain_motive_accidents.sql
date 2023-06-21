{{ config(materialized='table', schema='marts') }}
with is_rain_motive_accidents as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select distinct count(accident_id) as total_accidents
     , rain_intensity
  from is_rain_motive_accidents
  group by rain_intensity 
  order by total_accidents desc

