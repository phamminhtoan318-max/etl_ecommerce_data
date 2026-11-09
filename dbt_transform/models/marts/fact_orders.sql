with orders as (
    select * from {{ ref('stg_orders') }}
),

customers_stg as (
    select * from {{ ref('stg_customers') }}
),

items_agg as (
    select
        order_id,
        count(order_item_id)::integer as total_items_count,
        sum(item_price)::decimal(10, 2) as total_items_amount,
        sum(freight_value)::decimal(10, 2) as total_freight_amount,
        sum(item_price + freight_value)::decimal(10, 2) as total_order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments_agg as (
    select
        order_id,
        (array_agg(payment_type order by payment_value desc, payment_type asc, payment_sequential asc))[1] as payment_type,
        sum(payment_value)::decimal(10, 2) as total_payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews_agg as (
    select
        order_id,
        round(avg(review_score), 1)::decimal(3, 1) as review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}::uuid as order_key,
    o.order_id,
    coalesce(dc.customer_key, '00000000-0000-0000-0000-000000000000'::uuid) as customer_key,
    coalesce(dl.location_key, '00000000-0000-0000-0000-000000000000'::uuid) as shipping_location_key,
    case
        when o.purchased_at is not null then to_char(o.purchased_at, 'YYYYMMDD')::integer
        else -1
    end as purchase_date_key,
    case
        when o.approved_at is not null then to_char(o.approved_at, 'YYYYMMDD')::integer
        when o.order_status in ('canceled', 'unavailable') then -2
        else -3
    end as approved_date_key,
    case
        when o.delivered_to_customer_at is not null then to_char(o.delivered_to_customer_at, 'YYYYMMDD')::integer
        when o.order_status = 'delivered' and o.delivered_to_customer_at is null then -1
        when o.order_status in ('canceled', 'unavailable') then -2
        else -3
    end as delivered_date_key,
    o.order_status,
    coalesce(p.payment_type, 'unknown') as payment_type,
    case
        when o.order_status != 'delivered' or o.delivered_to_customer_at is null then 'Not Delivered'
        when o.delivered_to_customer_at <= o.estimated_delivery_at then 'On Time'
        else 'Late'
    end as delivery_status,
    coalesce(i.total_items_count, 0) as total_items_count,
    coalesce(i.total_items_amount, 0.00) as total_items_amount,
    coalesce(i.total_freight_amount, 0.00) as total_freight_amount,
    coalesce(i.total_order_amount, 0.00) as total_order_amount,
    coalesce(p.total_payment_value, 0.00) as total_payment_value,
    r.review_score
from orders o
left join customers_stg cs
    on o.customer_id = cs.customer_id
left join {{ ref('dim_location') }} dl
    on cs.customer_city = dl.city_name
    and cs.customer_state = dl.state_code
left join {{ ref('dim_customer') }} dc
    on cs.customer_unique_id = dc.customer_unique_id
    and (
        (o.purchased_at is not null and o.purchased_at >= dc.valid_from and o.purchased_at < dc.valid_to)
        or (o.purchased_at is null and dc.is_current = true)
    )
left join items_agg i
    on o.order_id = i.order_id
left join payments_agg p
    on o.order_id = p.order_id
left join reviews_agg r
    on o.order_id = r.order_id
