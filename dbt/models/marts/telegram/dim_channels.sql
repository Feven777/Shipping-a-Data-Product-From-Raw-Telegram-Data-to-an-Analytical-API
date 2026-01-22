{{ config(
    materialized = 'table'
) }}

with messages as (

    select
        channel,
        message_date,
        views
    from {{ ref('stg_telegram_messages') }}

),

aggregated as (

    select
        channel,
        min(message_date)  as first_post_date,
        max(message_date)  as last_post_date,
        count(*)           as total_messages,
        avg(views)         as avg_views
    from messages
    group by channel

)

select
    {{ dbt_utils.generate_surrogate_key(['channel']) }} as channel_key,
    channel                                              as channel_name,
    first_post_date,
    last_post_date,
    total_messages,
    avg_views
from aggregated
