with products as (
    select * from {{ ref('stg_products') }}
)

select
    md5(product_id)::uuid as product_key,
    product_id,
    product_category_name,
    round((product_weight_g / 1000.0)::numeric, 4) as weight_kg,
    round(
        ((product_length_cm * product_height_cm * product_width_cm) / 1000000.0)::numeric, 
        6
    ) as volume_m3
from products