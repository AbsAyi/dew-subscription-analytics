/*
    stg_recharge__customers
    
    Clean Recharge customer records. Key join field: shopify_customer_id
    enables mapping between Recharge and Shopify customer records.
    
    Grain: One row per customer
*/

with source as (
    select * from {{ source('recharge', 'raw_recharge__customers') }}
),

cleaned as (
    select
        id                                          as recharge_customer_id,
        shopify_customer_id,
        lower(trim(email))                          as customer_email,
        initcap(trim(first_name))                   as first_name,
        initcap(trim(last_name))                    as last_name,
        
        -- Note: Recharge timestamps may have timezone inconsistencies
        -- Some records are UTC, some are EST stored as UTC
        -- We accept as-is since the offset is small for customer-level analysis
        created_at::timestamp_ntz                   as recharge_created_at_utc,
        
        lower(status)                               as recharge_status,
        case 
            when lower(has_payment_method_in_dunning) = 'true' then true
            else false
        end                                         as is_in_dunning

    from source
)

select * from cleaned
