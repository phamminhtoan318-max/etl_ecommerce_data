with sales_sum as (
    select
        order_id,
        sum(item_price)::decimal(10, 2) as sum_sales_price
    from {{ ref('fact_sales') }}
    group by order_id
)

select
    coalesce(o.order_id, s.order_id) as order_id,
    coalesce(o.total_items_amount, 0.00) as total_items_amount,
    coalesce(s.sum_sales_price, 0.00) as sum_sales_price
from {{ ref('fact_orders') }} o
full outer join sales_sum s
    on o.order_id = s.order_id
where abs(coalesce(o.total_items_amount, 0.00) - coalesce(s.sum_sales_price, 0.00)) > 0.01
