with accounts_raw as (
    select * from {{ ref("stg_backend_data__accounts") }}
),
latest_object_counts as (
    select 
        * 
    from 
        {{ ref("stg_backend_data__object_counts_daily") }}
    qualify row_number() over (partition by account_id order by date desc) = 1 -- keep only the latest row for each account
),
enriched as (
    select 
        accounts_raw.*,
        latest_object_counts.* except(account_id, date),
        date as objects_count_last_update_date
    from 
        accounts_raw
    left join
        latest_object_counts
    on
        accounts_raw.account_id = latest_object_counts.account_id
)
select * from enriched