with source as (
    select * from {{ source('staging', 'olist_sellers_dataset') }}
),

renamed as (
    select
        seller_id,
        seller_zip_code_prefix as seller_zip_code,
        trim(lower(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state,
        _extracted_at
    from source
)

select * from renamed
