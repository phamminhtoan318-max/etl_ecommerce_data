{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert'
) }}

with source as (
    select * from {{ source('bronze', 'olist_products_dataset') }}
    {% if is_incremental() %}
        where _extracted_at > (
            select coalesce(max(_extracted_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
),

deduped as (
    select
        product_id,
        nullif(trim(product_category_name), '') as product_category_name,
        product_name_lenght as product_name_length,
        product_description_lenght as product_description_length,
        product_photos_qty as product_photos_quantity,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by product_id 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where product_id is not null
)

select
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_quantity,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
