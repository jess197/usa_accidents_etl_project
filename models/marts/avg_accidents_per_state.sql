{{ config(materialized='table') }}
with quantity_accidents_per_state as 
(
    select * from {{ ref('cleansed_usa_accidents') }}
)
select distinct state as accident_state_uf 
     , count(accident_id) as total_accidents
  from quantity_accidents_per_state
  group by accident_state_uf
  order by total_accidents desc