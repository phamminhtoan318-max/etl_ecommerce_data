{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}

with source as (
    select * from {{ source('bronze', 'olist_orders_dataset') }}
    {% if is_incremental() %}
        where _extracted_at > (
            select coalesce(max(_extracted_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
),

deduped as (
    select
        order_id,
        customer_id,
        lower(trim(order_status)) as order_status,
        order_purchase_timestamp as purchased_at,
        order_approved_at as approved_at,
        order_delivered_carrier_date as delivered_to_carrier_at,
        order_delivered_customer_date as delivered_to_customer_at,
        order_estimated_delivery_date as estimated_delivery_at,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by order_id 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where order_id is not null
)

select
    order_id,
    customer_id,
    order_status,
    purchased_at,
    approved_at,
    delivered_to_carrier_at,
    delivered_to_customer_at,
    estimated_delivery_at,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
