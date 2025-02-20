{{ config(materialized='table') }}

with fhv_data_2019 as (
    select * 
    from {{ ref('stg_fhv_data_2019') }}
),
dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
)

select
    fhv_data_2019.dispatching_base_num,
    fhv_data_2019.sr_flag,
    fhv_data_2019.dropoff_datetime,
    fhv_data_2019.pickup_datetime,
    fhv_data_2019.affiliated_base_number,  
    EXTRACT(YEAR FROM pickup_datetime) as year,
    EXTRACT(MONTH FROM pickup_datetime) as month,
    fhv_data_2019.pulocationid as pickup_location_id,
    fhv_data_2019.dolocationid as dropoff_location_id,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.zone as dropoff_zone
from fhv_data_2019
inner join dim_zones as pickup_zone
    on fhv_data_2019.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on fhv_data_2019.dolocationid = dropoff_zone.locationid
