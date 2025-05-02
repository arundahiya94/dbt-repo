-- models/dimensions/dim_stations.sql

{{ config(materialized='table') }}

with stations as (
  select distinct
    station_id,
    station_name  as name,
    lat,
    lon,
    address,
    cross_street,
    capacity,
    is_virtual_station,
    uri_android,
    uri_ios,
    uri_web
  from {{ ref('stg_station_information') }}
)

select *
from stations
order by station_id