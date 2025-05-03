{{ config(materialized="table") }}

select
    t.trip_date as date_key,
    t.start_station_id as station_id,
    count(*) as total_trips_started,
    avg(raw_duration_s) as avg_reported_duration_s,
    avg(computed_duration_s) as avg_computed_duration_s,
    sum(
        case when raw_duration_s <> computed_duration_s then 1 else 0 end
    ) as count_mismatched_durations
from {{ ref("fact_trips") }} t
group by 1, 2
