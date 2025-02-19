{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips_2019_2020') }}
)
    select 
    -- Revenue grouping 
    {{ convert_to_quarter('pickup_datetime') }} AS revenue_quarter,

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_quarter_fare,
    sum(extra) as revenue_quarter_extra,
    sum(mta_tax) as revenue_quarter_mta_tax,
    sum(tip_amount) as revenue_quarter_tip_amount,
    sum(tolls_amount) as revenue_quarter_tolls_amount,
    sum(ehail_fee) as revenue_quarter_ehail_fee,
    sum(improvement_surcharge) as revenue_quarter_improvement_surcharge,
    sum(total_amount) as revenue_quarter_total_amount,

    -- Additional calculations
    count(tripid) as total_quarter_trips,
    avg(passenger_count) as avg_quarter_passenger_count,
    avg(trip_distance) as avg_quarter_trip_distance

    from trips_data
    group by 1,2