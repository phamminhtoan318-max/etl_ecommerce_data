with source as (
    select * from {{ source('staging', 'olist_customers_dataset') }}
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        lower(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        _extracted_at
    from source
)

select * from renamed
