{{ config(
    materialized = 'table'
) }}

with detections as (

    select
        cast(message_id as bigint) as message_id,
        channel_name,
        object_name as image_category,
        confidence
    from {{ source('raw', 'yolo_detections') }}

),

messages as (

    select
        message_id,
        channel_key,
        date_key
    from {{ ref('fct_messages') }}

)

select
    d.message_id,
    m.channel_key,
    m.date_key,
    d.image_category,
    d.confidence
from detections d
join messages m
    on d.message_id = m.message_id
