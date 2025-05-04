-- models/facts/fact_station_uptime.sql
{{ config(materialized="table") }}

with
    history as (
        select
            station_id,
            feed_updated_at as status_timestamp,
            is_installed,
            is_renting,
            is_returning
        from {{ ref("stg_station_status") }}
    ),

    flagged as (
        select
            station_id,
            date(status_timestamp) as status_date,
            case
                when is_installed and is_renting and is_returning then 1 else 0
            end as up_flag,
            1 as record_count
        from history
    ),

    agg as (
        select
            station_id,
            status_date,
            sum(up_flag) as up_count,
            sum(record_count) as total_count,
            safe_divide(sum(up_flag), sum(record_count)) as uptime_pct
        from flagged
        group by station_id, status_date
    )

select *
from agg
