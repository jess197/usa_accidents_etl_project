{{ config(materialized='table') }}
with cleansed_usa_accidents as 
(
    select * from {{ ref('accidents_usa_raw') }}
)
select substring(ca.id,3,10) as accident_id 
     , substring(ca.source,7,7) as accident_source
     , ca.severity as accident_severity
     , date(ca.start_time) as accident_start_date 
     , time(ca.start_time) as accident_start_time
     , date(ca.end_time) as accident_end_date 
     , time(ca.end_time) as accident_end_time
     , CASE WHEN DATE_PART('dow', accident_start_date) = 0 THEN 'Sunday'
            WHEN DATE_PART('dow', accident_start_date) = 1 THEN 'Monday' 
            WHEN DATE_PART('dow', accident_start_date) = 2 THEN 'Tuesday'
            WHEN DATE_PART('dow', accident_start_date) = 3 THEN 'Wednesday'
            WHEN DATE_PART('dow', accident_start_date) = 4 THEN 'Thursday'
            WHEN DATE_PART('dow', accident_start_date) = 5 THEN 'Friday'
            WHEN DATE_PART('dow', accident_start_date) = 6 THEN 'Saturday'
        END AS accident_weekday
     , year(ca.start_time) as accident_year
     , ca.start_lat as latitude
     , ca.start_lng as longitude
     , ca.description as accident_description
     , ca.street 
     , ca.city 
     , ca.county
     , ca.state 
     , ca.zipcode 
     , ca.country
     , ca.temperature_f
     , ca.humidity 
     , ca.wind_direction 
     , ca.wind_speed_mph
     ,   CASE
            WHEN COALESCE(precipitation_in, 0) > 0 AND COALESCE(precipitation_in, 0) <= 0.25 THEN 'Light rain'
            WHEN COALESCE(precipitation_in, 0) > 0.25 AND COALESCE(precipitation_in, 0) <= 0.5 THEN 'Moderate rain'
            WHEN COALESCE(precipitation_in, 0) > 0.5 THEN 'Heavy rain'
            ELSE 'No precipitation'
        END AS rain_intensity
     , ca.weather_condition 
     , ca.amenity as nearby_amenity 
     , ca.bump as presence_road_bump 
     , ca.crossing_give_way  
     , ca.junction as presence_intersection 
     , ca.no_exit 
     , ca.railway  as presence_railway
     , ca.roundabout  as presence_roundabout
     , ca.station as nearby_transportation_station
     , ca.traffic_calming 
     , ca.traffic_signal as nearby_traffic_signal
     , ca.turning_loop as nearby_turning_loop
     , ca.civil_twilight
     , ca.nautical_twilight
     , ca.astronomical_twilight
  from cleansed_usa_accidents ca