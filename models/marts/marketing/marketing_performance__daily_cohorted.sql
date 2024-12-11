with accounts as (
    select * from {{ ref("stg_backend_data__accounts") }}
),
active_subscriptions_per_month as (
    select * from {{ ref("int_active_subscriptions_per_month") }}
)
/*
spend_per_month as (
    select * from int_spend_per_month
)
*/

-- This model joins revenue to spend
-- It is cohorted on signup_month
-- It allows the computation of metrics such as Number of Sign-ups, LTV, CAC, LTV/CAC, NPS, .. for a particular cohort

select 1
/*
Model logic here
*/