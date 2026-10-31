with source as (
    select * from {{ source('bronze', 'olist_order_items_dataset') }}
),

deduped as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_at,
        cast(price as decimal(10, 2)) as item_price,
        cast(freight_value as decimal(10, 2)) as freight_value,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by order_id, order_item_id 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where order_id is not null
      and order_item_id is not null
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_at,
    item_price,
    freight_value,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
