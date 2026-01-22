{{ config(
    materialized='table'
) }}

with messages as (

    select
        m.message_id,
        m.channel,
        m.message_date::date as full_date,
        m.message_text,
        m.views,
        m.forwards,
        m.has_media
    from {{ ref('stg_telegram_messages') }} as m

),

channels as (
    select
        channel_key,
        channel_name
    from {{ ref('dim_channels') }}
),

dates as (
    select
        date_key,
        full_date
    from {{ ref('dim_dates') }}
)

select
    m.message_id,
    c.channel_key,
    d.date_key,
    m.message_text,
    m.views,
    m.forwards,
    m.has_media
from messages m
left join channels c
    on m.channel = c.channel_name
left join dates d
    on m.full_date = d.full_date
