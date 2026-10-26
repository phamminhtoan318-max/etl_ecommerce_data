with all_locations as (
    select 
        customer_city as city_name, 
        customer_state as state_code
    from {{ ref('stg_customers') }}
    where customer_city is not null and customer_state is not null

    union

    select 
        seller_city as city_name, 
        seller_state as state_code
    from {{ ref('stg_sellers') }}
    where seller_city is not null and seller_state is not null
)

select
    md5(concat(city_name, '-', state_code))::uuid as location_key,
    city_name,
    state_code
from all_locations 