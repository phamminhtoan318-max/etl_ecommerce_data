{{ config(
    materialized='incremental',
    unique_key='seller_id',
    incremental_strategy='delete+insert'
) }}

with source as (
    select * from {{ source('bronze', 'olist_sellers_dataset') }}
    {% if is_incremental() %}
        where _extracted_at > (
            select coalesce(max(_extracted_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
),

deduped as (
    select
        seller_id,
        seller_zip_code_prefix as seller_zip_code,
        trim(lower(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by seller_id 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where seller_id is not null
)

select
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
