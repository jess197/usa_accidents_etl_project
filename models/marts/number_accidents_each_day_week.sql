{{ config(materialized='table') }}
with number_accidents_each_day_week as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select distinct count(accident_id) as total_accidents
     , accident_weekday
  from number_accidents_each_day_week
  group by accident_weekday 
  order by total_accidents desc