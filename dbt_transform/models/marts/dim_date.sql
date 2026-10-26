with date_series as (
    select 
        generate_series(
            '2016-01-01'::date, 
            '2020-12-31'::date, 
            '1 day'::interval
        )::date as full_date
)

select
    to_char(full_date, 'YYYYMMDD')::integer as date_key,
    full_date,
    extract(year from full_date)::integer as year,
    extract(quarter from full_date)::integer as quarter,
    extract(month from full_date)::integer as month,
    trim(to_char(full_date, 'Month'))::varchar as month_name,
    trim(to_char(full_date, 'Day'))::varchar as day_of_week_name,
    case 
        when extract(isodow from full_date) in (6, 7) then true 
        else false 
    end as is_weekend
from date_series