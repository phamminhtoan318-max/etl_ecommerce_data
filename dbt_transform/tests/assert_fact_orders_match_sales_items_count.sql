with sales_count as (
    select
        order_id,
        count(*) as actual_items_count
    from {{ ref('fact_sales') }}
    group by order_id
)

select
    coalesce(o.order_id, s.order_id) as order_id,
    coalesce(o.total_items_count, 0) as expected_items_count,
    coalesce(s.actual_items_count, 0) as actual_items_count
from {{ ref('fact_orders') }} o
full outer join sales_count s
    on o.order_id = s.order_id
where coalesce(o.total_items_count, 0) != coalesce(s.actual_items_count, 0)

