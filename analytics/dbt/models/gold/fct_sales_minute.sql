{{ config(materialized='view') }}
with base as (
  select
    date_trunc('minute', event_ts) as minute_bucket,
    sum(coalesce(total_amount,0)) as gmv,
    count(distinct order_id) as orders
  from bronze_orders
  where status in ('created','paid')
  group by 1
)
select * from base