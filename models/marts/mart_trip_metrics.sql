{{ config(materialized="table") }}

with
    raw as (
        select
            t.trip_date,
            concat('YOS:Station:', t.start_station_id) as station_id,
            t.raw_duration_s,
            t.computed_duration_s,
            case
                when t.raw_duration_s <> t.computed_duration_s then 1 else 0
            end as mismatched_count
        from {{ ref("fact_trips") }} t
    ),

    agg as (
        select
            trip_date as date_key,
            station_id,
            count(*) as total_trips_started,
            avg(raw_duration_s) as avg_reported_duration_s,
            avg(computed_duration_s) as avg_computed_duration_s,
            sum(mismatched_count) as count_mismatched_durations
        from raw
        group by 1, 2
    )

select a.*, ds.station_name  -- pull in the station name from your dim
from agg a
left join {{ ref("dim_stations") }} ds on a.station_id = ds.station_id
