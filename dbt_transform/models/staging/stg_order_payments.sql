with source as (
    select * from {{ source('bronze', 'olist_order_payments_dataset') }}
),

deduped as (
    select
        order_id,
        payment_sequential,
        lower(trim(payment_type)) as payment_type,
        payment_installments,
        cast(payment_value as decimal(10, 2)) as payment_value,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by order_id, payment_sequential 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where order_id is not null
      and payment_sequential is not null
)

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
