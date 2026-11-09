{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='delete+insert'
) }}

with source as (
    select * from {{ source('bronze', 'olist_customers_dataset') }}
    {% if is_incremental() %}
        where _extracted_at > (
            select coalesce(max(_extracted_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
),

deduped as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        lower(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by customer_id 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where customer_id is not null
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code,
    customer_city,
    customer_state,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
