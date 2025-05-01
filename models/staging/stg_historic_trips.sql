-- models/staging/stg_historic_trips.sql

with source as (
  select
    started_at,
    ended_at,
    duration         as duration_s,
    start_station_id,
    start_station_name,
    start_station_description,
    start_station_latitude   as start_lat,
    start_station_longitude  as start_lon,
    end_station_id,
    end_station_name,
    end_station_description,
    end_station_latitude     as end_lat,
    end_station_longitude    as end_lon
  from {{ source('trips','raw_historic_trips') }}
)

select
  *,
  date(started_at)       as trip_date,
  timestamp_trunc(started_at, HOUR) as trip_hour,
  timestamp_diff(ended_at, started_at, SECOND) as computed_duration_s
from source;