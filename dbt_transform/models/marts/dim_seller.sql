with sellers as (
    select * from {{ ref('stg_sellers') }}
),

locations as (
    select * from {{ ref('dim_location') }}
)

select
    md5(s.seller_id)::uuid as seller_key,
    s.seller_id,
    l.location_key
from sellers s
left join locations l
    on s.seller_city = l.city_name
    and s.seller_state = l.state_code