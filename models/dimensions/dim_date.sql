-- models/dimensions/dim_date.sql
{{ config(materialized="table") }}

with
    date_bounds as (
        select
            coalesce(
                (
                    select min(date(feed_updated_at))
                    from {{ ref("stg_station_status") }}
                ),
                date_sub(current_date(), interval 1 year)
            ) as start_date,
            current_date() as end_date
    ),

    all_dates as (
        select day
        from date_bounds, unnest(generate_date_array(start_date, end_date)) as day
    )

select
    day as date_key,
    extract(year from day) as year,
    extract(month from day) as month,
    extract(day from day) as day,
    extract(dayofweek from day) as weekday,  -- 1=Sunday … 7=Saturday
    format_date('%Y%m%d', day) as date_int
from all_dates
order by day
