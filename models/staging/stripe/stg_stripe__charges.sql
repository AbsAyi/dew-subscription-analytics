/*
    stg_stripe__charges
    
    Clean Stripe charge data. Converts amounts from cents to dollars.
    
    Note: Includes "ghost" failures — charges that failed in Stripe but have 
    no corresponding Recharge charge record. These represent payment processor 
    retries or system-level failures that Recharge didn't initiate.
    
    Grain: One row per charge attempt
*/

with source as (
    select * from {{ source('stripe', 'raw_stripe__charges') }}
),

cleaned as (
    select
        id                                          as stripe_charge_id,

        -- Convert cents to dollars (Stripe stores in smallest currency unit)
        {{ cents_to_dollars('amount') }}            as charge_amount,
        {{ cents_to_dollars('amount_refunded') }}   as refunded_amount,
        ({{ cents_to_dollars('amount') }} 
         - {{ cents_to_dollars('amount_refunded') }}) as net_charge_amount,

        upper(currency)                             as currency,
        status                                      as stripe_status,

        case 
            when status = 'succeeded' then true
            else false
        end                                         as is_successful,

        -- Timestamps (already UTC)
        created::timestamp_ntz                      as charge_created_at_utc,

        lower(trim(customer_email))                 as customer_email,

        -- Cross-reference IDs (may be null for ghost failures)
        case 
            when metadata_shopify_order_id is not null 
                 and trim(metadata_shopify_order_id::varchar) != ''
            then metadata_shopify_order_id
            else null
        end                                         as shopify_order_id,
        case 
            when metadata_recharge_charge_id is not null 
                 and trim(metadata_recharge_charge_id::varchar) != ''
            then metadata_recharge_charge_id
            else null
        end                                         as recharge_charge_id,

        -- Payment method
        lower(payment_method_type)                  as payment_method_type,

        -- Failure info
        case 
            when failure_code is not null and trim(failure_code) != ''
            then lower(trim(failure_code))
            else null
        end                                         as failure_code,

        -- Flag ghost failures (no Shopify or Recharge reference)
        case 
            when status = 'failed'
                 and (metadata_shopify_order_id is null or trim(metadata_shopify_order_id::varchar) = '')
                 and (metadata_recharge_charge_id is null or trim(metadata_recharge_charge_id::varchar) = '')
            then true
            else false
        end                                         as is_ghost_failure

    from source
)

select * from cleaned
