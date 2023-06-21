{{ config(materialized='table') }}
with accidents_per_city_per_period_day as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select nautical_twilight as period_of_day
     , city
     , count(accident_id) as total_accidents
  from accidents_per_city_per_period_day 
  group by nautical_twilight, city
  order by total_accidents desc