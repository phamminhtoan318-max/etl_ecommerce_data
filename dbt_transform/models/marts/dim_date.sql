with date_series as (
    select 
        generate_series(
            '2015-01-01'::date, 
            '2030-12-31'::date, 
            '1 day'::interval
        )::date as full_date
),

actual_dates as (
    select
        to_char(full_date, 'YYYYMMDD')::integer as date_key,
        full_date,
        extract(year from full_date)::integer as year,
        extract(quarter from full_date)::integer as quarter,
        extract(month from full_date)::integer as month,
        trim(to_char(full_date, 'FMMonth'))::varchar as month_name,
        trim(to_char(full_date, 'FMDay'))::varchar as day_of_week_name,
        case 
            when extract(isodow from full_date) in (6, 7) then true 
            else false 
        end as is_weekend
    from date_series
)

select * from actual_dates

union all

select
    -1 as date_key,
    null::date as full_date,
    1900 as year,
    0 as quarter,
    0 as month,
    'Unknown' as month_name,
    'Unknown' as day_of_week_name,
    false as is_weekend

union all

select
    -2 as date_key,
    null::date as full_date,
    1900 as year,
    0 as quarter,
    0 as month,
    'Not Applicable' as month_name,
    'Not Applicable' as day_of_week_name,
    false as is_weekend

union all

select
    -3 as date_key,
    null::date as full_date,
    1900 as year,
    0 as quarter,
    0 as month,
    'Pending' as month_name,
    'Pending' as day_of_week_name,
    false as is_weekend
