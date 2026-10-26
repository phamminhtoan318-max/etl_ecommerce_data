with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers_stg as (
    select * from {{ ref('stg_customers') }}
)

select
    md5(concat(order_id, '-', order_item_id))::uuid as sales_key,
    oi.order_id,
    oi.order_item_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    dl.location_key as shipping_location_key,
    to_char(o.purchased_at, 'YYYYMMDD')::integer as purchase_date_key,
    oi.item_price,
    oi.freight_value,
    (oi.item_price + oi.freight_value)::decimal(10, 2) as total_item_amount
from order_items oi
inner join orders o
    on oi.order_id = o.order_id
left join customers_stg cs
    on o.customer_id = cs.customer_id
left join {{ ref('dim_location') }} dl
    on cs.customer_city = dl.city_name
    and cs.customer_state = dl.state_code
left join {{ ref('dim_customer') }} dc
    on cs.customer_unique_id = dc.customer_unique_id
    and o.purchased_at >= dc.valid_from
    and o.purchased_at < dc.valid_to
left join {{ ref('dim_product') }} dp
    on oi.product_id = dp.product_id
left join {{ ref('dim_seller') }} ds
    on oi.seller_id = ds.seller_id