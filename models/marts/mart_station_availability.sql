{{ config(materialized="table") }}

select
    fs.status_date as date_key,
    fs.status_hour as hour_key,
    fs.station_id,
    ds.station_name,
    ds.lat,
    ds.lon,
    fs.bikes_available,
    fs.docks_available,
    (fs.bikes_available + fs.docks_available) as total_capacity,
    round(
        fs.bikes_available / nullif(fs.bikes_available + fs.docks_available, 0), 3
    ) as pct_bikes_available
from {{ ref("fact_station_status") }} fs
join {{ ref("dim_stations") }} ds using (station_id)
