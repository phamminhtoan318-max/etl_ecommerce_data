{{ config(
    warn_if = '> 0',
    error_if = '> 10'
) }}

select
    order_id,
    order_status,
    delivered_date_key
from {{ ref('fact_orders') }}
where order_status = 'delivered'
  and delivered_date_key is null
