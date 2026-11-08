{% snapshot snp_sellers %}

{{
    config(
      target_schema='silver',
      unique_key='seller_id',
      strategy='check',
      check_cols=['seller_zip_code', 'seller_city', 'seller_state'],
      invalidate_hard_deletes=True
    )
}}

select
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state,
    _batch_id,
    _extracted_at
from {{ ref('stg_sellers') }}

{% endsnapshot %}

