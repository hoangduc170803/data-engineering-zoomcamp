--Homework

{{config(materialized = 'view')}}


with fhv_data_2019 as (

    select * from {{ source('staging', 'fhv_data_2019') }}

)



    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from fhv_data_2019 
    where dispatching_base_num is not null

    -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}



