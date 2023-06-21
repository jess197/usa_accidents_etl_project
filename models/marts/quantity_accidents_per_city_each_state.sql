{{ config(materialized='table') }}
with quantity_accidents_per_city_each_state as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select ci.city
     , ci.state
     , count(ci.accident_id) as total_accidents
  from quantity_accidents_per_city_each_state as ci
  group by ci.city
     , ci.state
  order by total_accidents desc