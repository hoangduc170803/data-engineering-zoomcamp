{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips_2019_2020') }}
),  
aggregated as (
    select 
        {{ convert_to_quarter('pickup_datetime') }} as revenue_quarter,
        service_type, 
        sum(fare_amount) as revenue_quarter_fare,
        sum(extra) as revenue_quarter_extra,
        sum(mta_tax) as revenue_quarter_mta_tax,
        sum(tip_amount) as revenue_quarter_tip_amount,
        sum(tolls_amount) as revenue_quarter_tolls_amount,
        sum(ehail_fee) as revenue_quarter_ehail_fee,
        sum(improvement_surcharge) as revenue_quarter_improvement_surcharge,
        sum(total_amount) as revenue_quarter_total_amount,
        count(tripid) as total_quarter_trips,
        avg(passenger_count) as avg_quarter_passenger_count,
        avg(trip_distance) as avg_quarter_trip_distance
    from trips_data
    group by 1, 2
),  
split_quarter as (
    select
        service_type,
        revenue_quarter,
        revenue_quarter_total_amount,
        revenue_quarter_fare,
        revenue_quarter_extra,
        revenue_quarter_mta_tax,
        revenue_quarter_tip_amount,
        revenue_quarter_tolls_amount,
        revenue_quarter_ehail_fee,
        revenue_quarter_improvement_surcharge,
        total_quarter_trips,
        avg_quarter_passenger_count,
        avg_quarter_trip_distance,
        CAST(SPLIT(revenue_quarter, ' ')[OFFSET(0)] AS INT64) as year,
        CAST(REPLACE(SPLIT(revenue_quarter, ' ')[OFFSET(1)], 'Q', '') AS INT64) as quarter
    from aggregated
    WHERE CAST(SPLIT(revenue_quarter, ' ')[OFFSET(0)] AS INT64) IN (2019, 2020)
),  
growth as (
    select
        curr.service_type,
        curr.year,
        curr.quarter,
        curr.revenue_quarter as current_revenue_quarter,
        curr.revenue_quarter_total_amount,
        prev.revenue_quarter_total_amount as previous_year_total_amount,
        case
            when prev.revenue_quarter_total_amount is null or prev.revenue_quarter_total_amount = 0 then null
            else round(
                ((curr.revenue_quarter_total_amount - prev.revenue_quarter_total_amount) / CAST(prev.revenue_quarter_total_amount AS NUMERIC)) * 100,
                2
            )
        end as yoy_revenue_growth_pct,
        curr.revenue_quarter_fare,
        curr.revenue_quarter_extra,
        curr.revenue_quarter_mta_tax,
        curr.revenue_quarter_tip_amount,
        curr.revenue_quarter_tolls_amount,
        curr.revenue_quarter_ehail_fee,
        curr.revenue_quarter_improvement_surcharge,
        curr.total_quarter_trips,
        curr.avg_quarter_passenger_count,
        curr.avg_quarter_trip_distance
    from split_quarter curr
    left join split_quarter prev 
        on curr.service_type = prev.service_type
       and curr.quarter = prev.quarter
       and curr.year = prev.year + 1
)

select *
from growth 


--     dbt build --select +fct_taxi_trips_quarterly_revenue.sql --vars '{"is_test_run": false}'