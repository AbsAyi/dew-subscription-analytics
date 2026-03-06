/*
    stg_stripe__refunds
    
    Clean Stripe refund data. Converts cents to dollars.
    
    Note: Some refunds are "orphaned" — they exist in Stripe but the 
    corresponding Shopify order financial_status was never updated to 
    'refunded' or 'partially_refunded'. This is a reconciliation issue
    that must be accounted for in revenue reporting.
    
    Grain: One row per refund
*/

with source as (
    select * from {{ source('stripe', 'raw_stripe__refunds') }}
),

cleaned as (
    select
        id                                          as stripe_refund_id,
        charge_id                                   as stripe_charge_id,

        -- Convert cents to dollars
        {{ cents_to_dollars('amount') }}            as refund_amount,

        upper(currency)                             as currency,
        status                                      as refund_status,

        -- Timestamps (already UTC)
        created::timestamp_ntz                      as refund_created_at_utc,

        -- Reason
        case 
            when reason is not null and trim(reason) != ''
            then lower(trim(reason))
            else 'not_specified'
        end                                         as refund_reason,

        -- Cross-reference
        case 
            when metadata_shopify_order_id is not null 
                 and trim(metadata_shopify_order_id::varchar) != ''
            then metadata_shopify_order_id
            else null
        end                                         as shopify_order_id

    from source
)

select * from cleaned
