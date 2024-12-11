-- Same incremental logic as stg_events_data__usage_events

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

with accounts as (
    select * from {{ ref("stg_backend_data__accounts") }}
),
events as (
    select * from {{ ref("stg_events_data__usage_events") }}
) 

/*
 -- Pre-join events with dim tables such as accounts to enrich events
 -- Keep same granularity to be able to drill down in Looker
 */