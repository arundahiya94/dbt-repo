{{ config(materialized="table") }}

with
    raw as (
        select
            t.trip_date,
            concat('YOS:Station:', t.start_station_id) as start_station_id,
            concat('YOS:Station:', t.end_station_id) as end_station_id,
            t.start_station_name,
            t.end_station_name,
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
            start_station_id,
            end_station_id,
            start_station_name,
            end_station_name,
            count(*) as total_trips_started,
            avg(raw_duration_s) as avg_reported_duration_s,
            avg(computed_duration_s) as avg_computed_duration_s,
            sum(mismatched_count) as count_mismatched_durations
        from raw
        group by 1, 2, 3, 4, 5
    )

select
    a.*,

    -- cleaned names
    ds_start.station_name as cleaned_start_name,
    ds_end.station_name as cleaned_end_name,

    -- start station coordinates from the dim
    ds_start.lat as start_station_lat,
    ds_start.lon as start_station_lon,

    -- end station coordinates from the dim
    ds_end.lat as end_station_lat,
    ds_end.lon as end_station_lon

from agg a

left join {{ ref("dim_stations") }} ds_start on a.start_station_id = ds_start.station_id

left join {{ ref("dim_stations") }} ds_end on a.end_station_id = ds_end.station_id
