with seller_snapshots as (
    select
        seller_id,
        seller_city,
        seller_state,
        dbt_valid_from,
        dbt_valid_to,
        lag(dbt_valid_from) over (
            partition by seller_id
            order by dbt_valid_from
        ) as prev_valid_from
    from {{ ref('snp_sellers') }}
),

locations as (
    select * from {{ ref('dim_location') }}
),

scd_sellers as (
    select
        s.seller_id,
        coalesce(l.location_key, '00000000-0000-0000-0000-000000000000'::uuid) as location_key,
        case
            when s.prev_valid_from is null then '1900-01-01 00:00:00'::timestamp
            else s.dbt_valid_from::timestamp
        end as valid_from,
        coalesce(s.dbt_valid_to::timestamp, '9999-12-31 23:59:59'::timestamp) as valid_to,
        case
            when s.dbt_valid_to is null then true
            else false
        end as is_current
    from seller_snapshots s
    left join locations l
        on s.seller_city = l.city_name
        and s.seller_state = l.state_code
)

select
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'valid_from']) }}::uuid as seller_key,
    seller_id,
    location_key,
    valid_from,
    valid_to,
    is_current
from scd_sellers

union all

select
    '00000000-0000-0000-0000-000000000000'::uuid as seller_key,
    'unknown' as seller_id,
    '00000000-0000-0000-0000-000000000000'::uuid as location_key,
    '1900-01-01 00:00:00'::timestamp as valid_from,
    '9999-12-31 23:59:59'::timestamp as valid_to,
    true as is_current
