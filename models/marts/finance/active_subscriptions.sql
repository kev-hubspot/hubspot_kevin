{{
  config(
    materialized = 'table',
    partition_by={
      "field": "calendar_month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ["product", "subscription_phase"]
  )
}}

with active_subscriptions_per_month as (
    select * from {{ ref("int_active_subscriptions_per_month") }}
)

select
    calendar_month,
    subscription_id,
    account_id,
    product,
    tier,
    start_date,
    cancel_date,
    monthly_cost,
    is_touchless_conversion,
    subscription_lifetime_months,
    has_churned_this_month,
    is_active_startofmonth, 
    is_active_endofmonth,
    case
        when is_new_subscription then 'New'
        when has_churned_this_month then 'Churned'
        else 'Active'
    end as subscription_phase,
    case
        when is_new_customer then 'New'
        when account_total_active_subscriptions_endofmonth = 0 then 'Churned'
        else 'Active'
    end as customer_phase
from
    active_subscriptions_per_month