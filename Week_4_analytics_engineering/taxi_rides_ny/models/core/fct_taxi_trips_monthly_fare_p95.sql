{{ config(materialized='table') }}

with base as (
    select
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) as year,
        EXTRACT(MONTH FROM pickup_datetime) as month,
        fare_amount,
        trip_distance,
        payment_type_description
    from {{ ref('fact_trips_2019_2020') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
)

select
    service_type,
    year,
    month,
    -- APPROX_QUANTILES chia các giá trị fare_amount thành 101 bucket, lấy phần tử thứ 97 là 97th percentile.
    APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] as p97_fare,
    APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] as p95_fare,
    APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] as p90_fare
from base
WHERE year = 2020 AND month = 3
group by service_type, year, month

