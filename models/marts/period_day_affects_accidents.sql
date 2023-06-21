{{ config(materialized='table', schema='marts') }}
with period_day_affects_accidents as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select nautical_twilight as period_of_day
     , count(accident_id) as total_accidents
  from period_day_affects_accidents 
  group by nautical_twilight
  order by total_accidents desc