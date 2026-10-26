with sales_sum as (
    select
        order_id,
        sum(item_price)::decimal(10, 2) as sum_sales_price
    from {{ ref('fact_sales') }}
    group by order_id
)

select
    o.order_id,
    o.total_items_amount,
    s.sum_sales_price
from {{ ref('fact_orders') }} o
inner join sales_sum s
    on o.order_id = s.order_id
where abs(o.total_items_amount - s.sum_sales_price) > 0.01
