{{ config(
    materialized = 'table'
) }}

with dates as (

    select distinct
        message_date::date as full_date
    from {{ ref('stg_telegram_messages') }}

),

date_attributes as (

    select
        full_date,
        extract(day from full_date)       as day,
        extract(dow from full_date)       as day_of_week,
        extract(week from full_date)      as week_of_year,
        extract(month from full_date)     as month,
        extract(quarter from full_date)   as quarter,
        extract(year from full_date)      as year,
        case
            when extract(dow from full_date) in (0, 6) then true
            else false
        end                               as is_weekend
    from dates

)

select
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} as date_key,
    full_date,
    day,
    day_of_week,
    week_of_year,
    month,
    quarter,
    year,
    is_weekend
from date_attributes
