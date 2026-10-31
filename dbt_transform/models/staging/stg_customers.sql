with source as (
    select * from {{ source('bronze', 'olist_customers_dataset') }}
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
