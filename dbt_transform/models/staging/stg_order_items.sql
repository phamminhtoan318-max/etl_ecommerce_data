with source as (
    select * from {{ source('staging', 'olist_order_items_dataset') }}
),

renamed as (
    select
        md5(concat(order_id, '-', order_item_id)) as order_item_sk,
        order_id,
        order_item_id as order_item_seq,
        product_id,
        seller_id,
        shipping_limit_date as shipping_limit_at,
        cast(price as decimal(10, 2)) as item_price,
        cast(freight_value as decimal(10, 2)) as freight_value,
        _extracted_at
    from source
)

select * from renamed
