/*
    stg_recharge__subscriptions
    
    Clean subscription data and resolve status mismatches.
    
    Key transformation: If a subscription has status='active' but a non-null
    cancelled_at date, we resolve to 'cancelled'. This is a known Recharge 
    data quality issue where the status field doesn't update on cancellation.
    
    Grain: One row per subscription
*/

with source as (
    select * from {{ source('recharge', 'raw_recharge__subscriptions') }}
),

cleaned as (
    select
        id                                          as subscription_id,
        customer_id                                 as recharge_customer_id,
        shopify_customer_id,
        lower(trim(email))                          as customer_email,
        
        -- Product
        product_title,
        variant_title,
        sku,
        price::decimal(10,2)                        as subscription_price,
        quantity::integer                            as quantity,

        -- Status: resolve mismatches
        -- If cancelled_at is populated but status says 'active', trust cancelled_at
        case
            when status = 'active' 
                 and cancelled_at is not null 
                 and trim(cancelled_at) != '' 
            then 'cancelled'
            else lower(status)
        end                                         as subscription_status,

        -- Flag the mismatch for data quality reporting
        case
            when status = 'active' 
                 and cancelled_at is not null 
                 and trim(cancelled_at) != '' 
            then true
            else false
        end                                         as had_status_mismatch,

        -- Timestamps (already UTC from Recharge)
        created_at::timestamp_ntz                   as subscription_created_at_utc,
        case 
            when cancelled_at is not null and trim(cancelled_at) != '' 
            then cancelled_at::timestamp_ntz
            else null
        end                                         as subscription_cancelled_at_utc,

        -- Cancellation
        case 
            when cancellation_reason is not null and trim(cancellation_reason) != ''
            then lower(trim(cancellation_reason))
            else null
        end                                         as cancellation_reason,

        -- Subscription cadence
        charge_interval_frequency::integer          as charge_interval_days,
        lower(order_interval_unit)                  as charge_interval_unit,
        case 
            when charge_interval_frequency::integer <= 30 then '30-day'
            when charge_interval_frequency::integer <= 90 then '90-day'
            else 'other'
        end                                         as subscription_frequency_tier,

        -- Next charge
        case 
            when next_charge_scheduled_at is not null and trim(next_charge_scheduled_at) != ''
            then next_charge_scheduled_at::timestamp_ntz
            else null
        end                                         as next_charge_at_utc

    from source
)

select * from cleaned
