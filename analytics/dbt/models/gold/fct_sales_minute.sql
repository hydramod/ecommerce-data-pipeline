-- File: analytics/dbt/models/gold/fct_sales_minute.sql
{{ config(
    materialized='incremental',
    unique_key='minute_bucket',
    incremental_strategy='merge'  -- Explicit strategy
) }}

with base as (
  select
    date_trunc('minute', event_ts) as minute_bucket,
    sum(coalesce(total_amount,0)) as gmv,
    count(distinct order_id) as orders
  from {{ source('lake', 'orders_clean') }}
  {% if is_incremental() %}
    -- More robust: process data from the last 2 hours to ensure no gaps on failure
    where event_ts >= (select coalesce(max(minute_bucket) - interval '2 hours', timestamp '1970-01-01') from {{ this }})
  {% endif %}
  group by 1
)

select 
  minute_bucket,
  gmv,
  orders,
  current_timestamp as processed_ts  -- Added for auditing
from base