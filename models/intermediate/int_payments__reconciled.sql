/*
    Matches Shopify orders to Stripe charges.
    Flags discrepancies: orphaned refunds, amount mismatches, unmatched orders.
    Grain: One row per Shopify order.
*/
with shopify_orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

stripe_charges as (
    select * from {{ ref('stg_stripe__charges') }}
),

stripe_refunds as (
    select
        shopify_order_id,
        sum(refund_amount) as total_refund_amount,
        count(*) as refund_count
    from {{ ref('stg_stripe__refunds') }}
    where shopify_order_id is not null
    group by shopify_order_id
),

reconciled as (
    select
        so.order_id,
        so.order_created_at_utc,
        date_trunc('month', so.order_created_at_utc) as order_month,
        so.shopify_customer_id,
        so.gross_revenue as shopify_gross_revenue,
        so.financial_status as shopify_financial_status,

        sc.stripe_charge_id,
        sc.charge_amount as stripe_charge_amount,
        sc.stripe_status,
        sc.is_successful as stripe_is_successful,

        coalesce(sr.total_refund_amount, 0) as stripe_refund_amount,
        coalesce(sr.refund_count, 0) as stripe_refund_count,
        coalesce(sc.charge_amount, 0)
            - coalesce(sr.total_refund_amount, 0) as stripe_net_settled,

        case
            when sc.stripe_charge_id is null then 'no_stripe_match'
            when abs(so.gross_revenue - sc.charge_amount) > 0.01 then 'amount_mismatch'
            when sr.total_refund_amount > 0
                 and so.financial_status not in ('refunded', 'partially_refunded')
            then 'orphaned_refund'
            else 'reconciled'
        end as reconciliation_status,

        case
            when sc.charge_amount is not null
            then so.gross_revenue - sc.charge_amount
            else null
        end as revenue_variance

    from shopify_orders so
    left join stripe_charges sc
        on so.order_id = sc.shopify_order_id
        and sc.is_successful = true
    left join stripe_refunds sr
        on so.order_id = sr.shopify_order_id
)

select * from reconciled