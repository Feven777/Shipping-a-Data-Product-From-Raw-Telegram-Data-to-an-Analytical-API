{{ config(
    materialized='view'
) }}

with source as (

    select
        message_id,
        channel,
        message_date,
        message_text,
        views,
        forwards,
        has_media,
        media_file,
        raw_json,
        ingestion_timestamp,
        source_file,
        load_batch_id

    from raw.telegram_messages

)

select
    message_id::bigint                as message_id,
    channel::text                     as channel,
    message_date::timestamptz         as message_date,
    message_text::text                as message_text,
    coalesce(views, 0)::integer       as views,
    coalesce(forwards, 0)::integer    as forwards,
    has_media::boolean                as has_media,
    media_file::text                  as media_file,
    raw_json                          as raw_json,
    ingestion_timestamp::timestamptz  as ingestion_timestamp,
    source_file::text                 as source_file,
    load_batch_id::uuid               as load_batch_id

from source
