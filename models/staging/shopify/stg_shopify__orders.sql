/*
    stg_shopify__orders
    
    Cleans and deduplicates Shopify order data.
    
    Key transformations:
    - Deduplicates webhook retry records (same order_id, slightly different timestamps)
    - Converts EST timestamps to UTC for consistency with Recharge/Stripe
    - Parses UTM parameters from landing_site URL
    - Casts price fields from string to numeric
    
    Grain: One row per unique order
*/

with source as (
    select * from {{ source('shopify', 'raw_shopify__orders') }}
),

-- Deduplicate webhook retries: keep the earliest record per order_id
deduplicated as (
    select
        *,
        row_number() over (
            partition by id 
            order by created_at asc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Keys
        id                                          as order_id,
        order_number,
        customer_id                                 as shopify_customer_id,
        lower(trim(email))                          as customer_email,

        -- Timestamps: convert EST to UTC
        -- Shopify stores timestamps in shop timezone (EST/UTC-5)
        convert_timezone(
            'America/New_York', 
            'UTC', 
            created_at::varchar::timestamp_ntz
        )                                           as order_created_at_utc,

        -- Financials
        subtotal_price::decimal(10,2)               as subtotal_amount,
        total_tax::decimal(10,2)                    as tax_amount,
        total_shipping::decimal(10,2)               as shipping_amount,
        total_price::decimal(10,2)                  as gross_revenue,
        total_discounts::decimal(10,2)              as discount_amount,
        (total_price::decimal(10,2) 
         - total_discounts::decimal(10,2))          as net_revenue,

        -- Status
        financial_status,
        fulfillment_status,
        
        -- Order metadata
        source_name,
        case 
            when source_name = 'subscription_contract' then true
            else false
        end                                         as is_subscription_order,
        discount_codes,
        tags,
        billing_address_province                    as billing_state,

        -- Attribution: parse UTM params from landing_site
        -- Real-world problem: repeat subscription orders often have null attribution
        case 
            when landing_site like '%utm_source=%' 
            then split_part(split_part(landing_site, 'utm_source=', 2), '&', 1)
            else null
        end                                         as utm_source,
        case 
            when landing_site like '%utm_medium=%' 
            then split_part(split_part(landing_site, 'utm_medium=', 2), '&', 1)
            else null
        end                                         as utm_medium,

        -- Cancellation
        cancel_reason,
        cancelled_at

    from deduplicated
    where row_num = 1  -- Keep only first occurrence of each order
)

select * from cleaned
