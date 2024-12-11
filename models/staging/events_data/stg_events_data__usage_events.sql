-- Events tables are generally huge. This model is materialized as incremental so that only new data is processed on each run
-- On each incremental run, the last 3 days of events (+ current_date) will be dropped and replaced
-- This is to account. for late arriving events

{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)',
  'date_sub(current_date, interval 2 day)',
  'date_sub(current_date, interval 3 day)'
] %}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partitions = partitions_to_replace,
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["event_type", "product"]
  )
}}

with events_transformed as (
    select
        account_id,
        user_id,
        event_id,
        event_time,
        date(event_time) as event_date, 
        product,
        event_type,
        lead(event_time) over (partition by account_id, user_id order by event_time) as next_event_time, 
        -- define session dimensions here
        -- other transformations on the raw events
    from
        {{ source('events_data', 'usage_events') }}
    {% if is_incremental() %}
    where 
        date(event_time) in ({{ partitions_to_replace | join(',') }}) -- The source table (usage_events) is partitioned by event_time with day granularity
    {% endif %}
    qualify row_number() over (partition by event_id) = 1 -- Remove duplicate events. Usually we don't filter on stg models but it's more efficient to dedupe early on events data
)
select * from events_transformed