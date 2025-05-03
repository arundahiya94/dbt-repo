{{ config(materialized="table") }}

with
    trips as (
        select
            t.trip_id,
            t.trip_date as date_key,
            extract(hour from t.started_at) as hour_of_day,
            t.start_station_id,
            t.end_station_id,
            t.computed_duration_s
        from {{ ref("fact_trips") }} t
    )

select
    date_key,
    hour_of_day,
    start_station_id,
    ds1.station_name as start_station_name,
    end_station_id,
    ds2.station_name as end_station_name,
    count(*) as trip_count,
    avg(computed_duration_s) as avg_duration_s,
    approx_percentile(computed_duration_s, 0.5) as median_duration_s
from trips
left join {{ ref("dim_stations") }} ds1 on trips.start_station_id = ds1.station_id
left join {{ ref("dim_stations") }} ds2 on trips.end_station_id = ds2.station_id
group by
    date_key,
    hour_of_day,
    start_station_id,
    ds1.station_name,
    end_station_id,
    ds2.station_name
