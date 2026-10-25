with source as (
    select * from {{ source('staging', 'olist_order_reviews_dataset') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        _extracted_at
    from source
)

select * from renamed
