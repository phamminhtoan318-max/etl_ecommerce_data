select
    order_id,
    purchase_date_key,
    approved_date_key,
    delivered_date_key
from {{ ref('fact_orders') }}
where purchase_date_key > 0
  and (
      (approved_date_key > 0 and approved_date_key < purchase_date_key)
      or (delivered_date_key > 0 and delivered_date_key < purchase_date_key)
  )
