with products as (
    select * from {{ ref('stg_products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}::uuid as product_key,
    product_id,
    product_category_name,
    round((product_weight_g / 1000.0)::numeric, 4) as weight_kg,
    round(
        ((product_length_cm * product_height_cm * product_width_cm) / 1000000.0)::numeric, 
        6
    ) as volume_m3
from products

union all

select
    '00000000-0000-0000-0000-000000000000'::uuid as product_key,
    'unknown' as product_id,
    'unknown' as product_category_name,
    0.0000 as weight_kg,
    0.000000 as volume_m3
