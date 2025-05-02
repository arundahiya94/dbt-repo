-- models/staging/stg_station_status.sql
with
    source as (
        select
            ingest_datetime,
            last_updated as feed_last_updated,  -- integer epoch seconds
            ttl,
            version,
            s.*  -- all the station-level fields
        from {{ source("gbfs", "raw_station_status") }}, unnest(data.stations) as s
    )

select
    station_id,

    -- convert epoch seconds → TIMESTAMP
    timestamp_seconds(last_reported) as reported_at,

    -- only the fields present in your feed
    num_bikes_available as bikes_available,
    num_docks_available as docks_available,

    is_installed,
    is_renting,
    is_returning,

    -- already a TIMESTAMP
    ingest_datetime as ingest_at,

    -- convert feed’s last_updated epoch → TIMESTAMP
    timestamp_seconds(feed_last_updated) as feed_updated_at,

    version,
    ttl

from source
