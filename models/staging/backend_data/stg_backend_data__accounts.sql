with source as (
    select * from {{ source('backend_data', 'accounts') }}
),

renamed as (
    select
        id as account_id,
        name as account_name,
        signup_date,
        date_trunc(signup_date, month) as signup_month,
        country,
        vertical,
        nps
    from 
        source
)

select * from renamed
