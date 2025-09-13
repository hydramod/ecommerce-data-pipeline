{{ config(materialized='view') }}

select *
from {{ ref('fct_sales_minute') }}
where minute_bucket >= current_timestamp - interval '{{ var("window_minutes", 60) }}' minute
