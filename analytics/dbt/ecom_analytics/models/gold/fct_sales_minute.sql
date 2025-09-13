-- models/gold/fct_sales_minute.sql
{{ config(
    materialized='incremental',
    unique_key='minute_bucket',
    incremental_strategy='merge'
) }}

with orders as (
  select
    cast(order_id as bigint)  as order_id,
    cast(total_amount as double) as total_amount,
    event_ts
  from {{ source('silver', 'orders_clean') }}
  where event_ts is not null
),
enriched as (
  select
    cast(order_id as bigint) as order_id,
    fully_paid
  from {{ source('silver', 'order_payments_enriched') }}
),
paid_orders as (
  select o.event_ts, o.total_amount
  from orders o
  join enriched e using (order_id)
  where e.fully_paid = true
)

select
  date_trunc('minute', event_ts) as minute_bucket,
  sum(total_amount)               as gmv,
  count(*)                        as paid_orders,
  current_timestamp               as processed_ts
from paid_orders
{% if is_incremental() %}
where event_ts >= (
  select coalesce(max(minute_bucket) - interval '2' hour, timestamp '1970-01-01')
  from {{ this }}
)
{% endif %}
group by 1
