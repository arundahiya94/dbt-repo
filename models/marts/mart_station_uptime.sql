{{ config(materialized="table") }}

with
    snaps as (
        select
            s.station_id, ds.station_name, s.is_installed, s.is_renting, s.is_returning
        from {{ ref("stg_station_status") }} s
        join {{ ref("dim_stations") }} ds using (station_id)
    )

select
    station_id,
    station_name,
    count(*) as total_snapshots,
    countif(is_installed = 1) as installed_snapshots,
    countif(is_renting = 1) as renting_snapshots,
    countif(is_returning = 1) as returning_snapshots,
    round(countif(is_renting = 1) / count(*), 3) as pct_time_renting,
    round(countif(is_returning = 1) / count(*), 3) as pct_time_returning
from snaps
group by station_id, station_name
