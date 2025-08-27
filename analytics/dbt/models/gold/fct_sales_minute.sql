{{ config(materialized='incremental', unique_key='minute_bucket') }}

with base as (
  select
    date_trunc('minute', event_ts) as minute_bucket,
    sum(coalesce(total_amount,0)) as gmv,
    count(distinct order_id) as orders
  from hive.default.orders_clean
  {% if is_incremental() %}
    where event_ts > (select coalesce(max(minute_bucket), timestamp '1970-01-01') from {{ this }})
  {% endif %}
  group by 1
)
select * from base
