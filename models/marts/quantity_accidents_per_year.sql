{{ config(materialized='table') }}
with quantity_accidents_per_year as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select accident_year
     , count(accident_id) as total_accidents
  from quantity_accidents_per_year
  group by accident_year
  order by total_accidents desc