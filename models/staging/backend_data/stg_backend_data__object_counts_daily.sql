with source as (
    select * from {{ source('backend_data', 'object_counts_daily') }}
),
renamed as (
    select
        account_id,
        date,
        total_contacts,
        total_lists,
        total_deals,
        total_landing_pages,
        total_blogs,
        total_workflows
    from source
)

select * from renamed
