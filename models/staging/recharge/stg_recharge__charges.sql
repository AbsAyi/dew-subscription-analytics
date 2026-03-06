/*
    stg_recharge__charges
    
    Clean charge attempt data. Includes both successful and failed charges.
    
    Key context: Failed charges may not have a Shopify order ID because
    the order was never created. This is expected behavior, not a data error.
    
    Grain: One row per charge attempt
*/

with source as (
    select * from {{ source('recharge', 'raw_recharge__charges') }}
),

cleaned as (
    select
        id                                          as charge_id,
        subscription_id,
        customer_id                                 as recharge_customer_id,
        case 
            when shopify_order_id is not null and trim(shopify_order_id::varchar) != ''
            then shopify_order_id
            else null
        end                                         as shopify_order_id,
        lower(trim(email))                          as customer_email,

        -- Timestamps (already UTC)
        created_at::timestamp_ntz                   as charge_created_at_utc,
        case 
            when processed_at is not null and trim(processed_at) != ''
            then processed_at::timestamp_ntz
            else null
        end                                         as charge_processed_at_utc,

        -- Charge details
        upper(type)                                 as charge_type,  -- CHECKOUT or RECURRING
        upper(status)                               as charge_status,  -- SUCCESS, ERROR, DECLINED
        
        case 
            when upper(status) = 'SUCCESS' then true
            else false
        end                                         as is_successful_charge,

        case 
            when upper(status) in ('ERROR', 'DECLINED') then true
            else false
        end                                         as is_failed_charge,

        -- Financials
        total_price::decimal(10,2)                  as charge_total,
        subtotal_price::decimal(10,2)               as charge_subtotal,
        tax_lines::decimal(10,2)                    as charge_tax,
        discount_codes,

        -- Error handling
        case 
            when error_message is not null and trim(error_message) != ''
            then trim(error_message)
            else null
        end                                         as error_message,

        case 
            when error_message ilike '%declined%' then 'card_declined'
            when error_message ilike '%insufficient%' then 'insufficient_funds'
            when error_message ilike '%expired%' then 'card_expired'
            when error_message ilike '%update%' then 'payment_method_update_needed'
            when error_message is not null and trim(error_message) != '' then 'other_error'
            else null
        end                                         as error_category,

        lower(payment_processor)                    as payment_processor

    from source
)

select * from cleaned
