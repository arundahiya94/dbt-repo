{{ config(materialized="table") }}

with
    snaps as (
        select s.snapshot_id, s.station_id, s.is_installed, s.is_renting, s.is_returning
        from {{ ref("fact_station_status_history") }} s
    )

select
    station_id,
    count(*) as total_snapshots,
    countif(is_installed) as installed_snapshots,
    countif(is_renting) as renting_snapshots,
    countif(is_returning) as returning_snapshots,
    round(countif(is_renting) / count(*), 3) as pct_time_renting,
    round(countif(is_returning) / count(*), 3) as pct_time_returning
from snaps
group by station_id
