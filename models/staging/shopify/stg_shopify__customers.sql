/*
    stg_shopify__customers
    
    Clean and standardize Shopify customer records.
    
    Grain: One row per customer
*/

with source as (
    select * from {{ source('shopify', 'raw_shopify__customers') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by lower(trim(email))
            order by created_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        id                                          as shopify_customer_id,
        lower(trim(email))                          as customer_email,
        initcap(trim(first_name))                   as first_name,
        initcap(trim(last_name))                    as last_name,
        upper(trim(state))                          as billing_state,
        convert_timezone(
            'America/New_York', 
            'UTC', 
            created_at::varchar::timestamp_ntz
        )                                           as customer_created_at_utc,
        orders_count::integer                       as shopify_orders_count,
        total_spent::decimal(10,2)                  as shopify_total_spent,
        case 
            when lower(accepts_marketing) = 'true' then true
            else false
        end                                         as accepts_marketing,
        tags                                        as customer_tags

    from deduplicated
    where row_num = 1
)

select * from cleaned
