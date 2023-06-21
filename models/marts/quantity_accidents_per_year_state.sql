{{ config(materialized='table') }}
with quantity_accidents_per_year_state as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select distinct state as accident_state_uf 
     , accident_year
     , count(accident_id) as total_accidents
  from quantity_accidents_per_year_state
  group by accident_year, accident_state_uf
  order by total_accidents desc