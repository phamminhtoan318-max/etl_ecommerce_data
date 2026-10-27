with customer_orders_raw as (
    select
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        coalesce(o.purchased_at, '2016-01-01'::timestamp) as purchased_at,
        row_number() over (
            partition by c.customer_unique_id, coalesce(o.purchased_at, '2016-01-01'::timestamp)
            order by c.customer_id
        ) as rn
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_orders') }} o
        on c.customer_id = o.customer_id
),

customer_orders as (
    select
        customer_unique_id,
        customer_city,
        customer_state,
        purchased_at
    from customer_orders_raw
    where rn = 1
),

location_mapped as (
    select
        co.customer_unique_id,
        l.location_key,
        co.purchased_at,
        lag(l.location_key) over (
            partition by co.customer_unique_id
            order by co.purchased_at
        ) as prev_location_key
    from customer_orders co
    left join {{ ref('dim_location') }} l
        on co.customer_city = l.city_name
        and co.customer_state = l.state_code
),

scd_changes as (
    select
        customer_unique_id,
        location_key,
        purchased_at as valid_from
    from location_mapped
    where prev_location_key is null
       or prev_location_key != location_key
),

scd_calculated as (
    select
        customer_unique_id,
        location_key,
        valid_from,
        lead(valid_from) over (
            partition by customer_unique_id
            order by valid_from
        ) as next_valid_from
    from scd_changes
)

select
    md5(concat(customer_unique_id, '-', valid_from::text))::uuid as customer_key,
    customer_unique_id,
    location_key,
    valid_from,
    coalesce(next_valid_from, '9999-12-31'::timestamp) as valid_to,
    case
        when next_valid_from is null then true
        else false
    end as is_current
from scd_calculated
