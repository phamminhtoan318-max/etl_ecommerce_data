{{ config(
    materialized='incremental',
    unique_key=['review_id', 'order_id'],
    incremental_strategy='delete+insert'
) }}

with source as (
    select * from {{ source('bronze', 'olist_order_reviews_dataset') }}
    {% if is_incremental() %}
        where _extracted_at > (
            select coalesce(max(_extracted_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
),

deduped as (
    select
        review_id,
        order_id,
        review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        _batch_id,
        _extracted_at,
        row_number() over (
            partition by review_id, order_id 
            order by _extracted_at desc, _batch_id desc
        ) as _row_num
    from source
    where review_id is not null
      and order_id is not null
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    _batch_id,
    _extracted_at
from deduped
where _row_num = 1
