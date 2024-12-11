with source as (
    select * from {{ source('backend_data', 'subscriptions') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['account_id', 'product']) }} as subscription_id, 
        account_id, 
        product, 
        tier, 
        start_date, 
        date_trunc(start_date, month) as start_month,
        cancel_date, 
        date_trunc(cancel_date, month) as cancel_month,
        monthly_cost, 
        monthly_cost * 12 as annual_cost,
        touchless_conversion as is_touchless_conversion,
        cancel_date is null as is_active,
        -- Assumption: The subscription table contains a row per account_id x product x tier
        row_number() over (partition by account_id order by start_date) as account_subscription_order,
        lag(tier) over (partition by account_id, product order by start_date) as account_product_previous_tier,
        lead(tier) over (partition by account_id, product order by start_date) as account_product_next_tier,
        lag(monthly_cost) over (partition by account_id, product order by start_date) as account_product_previous_monthly_cost,
    from 
        source
)

select * from renamed
