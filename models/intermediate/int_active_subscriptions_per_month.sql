with subscriptions as (
   select * from {{ ref("stg_backend_data__subscriptions") }}
),
calendar_months as (
    select * from {{ ref("int_calendar_months") }}
),
-- For each subscription, create one row per active month (i.e not cancelled)
subscriptions_per_month as (
    select
        *,
        date_diff(calendar_month, start_month, month) as subscription_lifetime_months, 
         -- on the next rows, account_product_next_tier/account_product_previous_tier are used to check for subscription tier changes (upgrades or downgrades)
        coalesce(calendar_month = cancel_month and account_product_next_tier is null, false) as has_churned_this_month, -- account_product_next_tier is used to check for tier upgrades
        coalesce(calendar_month = cancel_month and account_product_next_tier is not null, false) as has_tier_changed_this_month,
        calendar_month <= cancel_month or cancel_month is null as is_active_startofmonth, -- subscription is active at the start of the month
        calendar_month < cancel_month or cancel_month is null as is_active_endofmonth,
        calendar_month = start_month and account_product_previous_tier is null as is_new_subscription,
        calendar_month = start_month and account_subscription_order = 1 as is_new_customer,         
    from
        subscriptions
    cross join
        calendar_months
    where
        calendar_month >= start_month
        and (calendar_month <= cancel_month or cancel_month is null)
),
final as (
    select
        *,
        sum(case when is_active_endofmonth then 1 else 0 end) over (partition by account_id, calendar_month) as account_total_active_subscriptions_endofmonth
    from
        subscriptions_per_month
)

select * from final
