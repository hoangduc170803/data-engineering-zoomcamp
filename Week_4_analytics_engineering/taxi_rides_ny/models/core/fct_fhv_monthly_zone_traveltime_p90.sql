{{ config(materialized='table') }}

with trips as (
    -- Reference your core model (dim_fhv_trips)
    select *
    from {{ ref('dim_fhv_trips') }}
),
trip_durations as (
    select
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        -- Compute trip duration in seconds
        TIMESTAMP_DIFF(trips.dropoff_datetime, trips.pickup_datetime, SECOND) as trip_duration
    from trips
)

select
    year,
    month,
    pickup_location_id,
    dropoff_location_id,
    -- APPROX_QUANTILES divides the trip_duration values into 101 buckets.
    -- The element at OFFSET(90) approximates the continuous 90th percentile.
    APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] as p90_travel_time
from trip_durations
group by year, month, pickup_location_id, dropoff_location_id
order by p90_travel_time desc
