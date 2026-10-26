select
    sales_key,
    order_id,
    item_price,
    freight_value,
    total_item_amount
from {{ ref('fact_sales') }}
where item_price < 0
   or freight_value < 0
   or total_item_amount < 0
