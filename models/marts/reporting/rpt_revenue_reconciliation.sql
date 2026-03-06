/*
    Monthly Shopify vs Stripe revenue. Flags variance > 2%.
    Grain: One row per month.
*/
with reconciled_orders as (
    select * from {{ ref('int_payments__reconciled') }}
),

monthly as (
    select
        order_month,
        sum(shopify_gross_revenue) as shopify_revenue,
        count(*) as total_orders,
        sum(case when stripe_is_successful then stripe_charge_amount else 0 end)
            as stripe_gross_revenue,
        sum(stripe_refund_amount) as stripe_total_refunds,
        sum(stripe_net_settled) as stripe_net_settled,
        sum(case when reconciliation_status = 'reconciled' then 1 else 0 end)
            as reconciled_orders,
        sum(case when reconciliation_status = 'no_stripe_match' then 1 else 0 end)
            as unmatched_orders,
        sum(case when reconciliation_status = 'amount_mismatch' then 1 else 0 end)
            as amount_mismatches,
        sum(case when reconciliation_status = 'orphaned_refund' then 1 else 0 end)
            as orphaned_refunds
    from reconciled_orders
    group by order_month
),

final as (
    select
        order_month,
        total_orders,
        shopify_revenue,
        stripe_gross_revenue,
        stripe_total_refunds,
        stripe_net_settled,
        shopify_revenue - stripe_gross_revenue as gross_revenue_variance,
        {{ safe_divide('(shopify_revenue - stripe_gross_revenue)', 'shopify_revenue') }}
            as variance_pct,
        case
            when abs({{ safe_divide('(shopify_revenue - stripe_gross_revenue)', 'shopify_revenue') }}) > 0.02
            then true else false
        end as exceeds_variance_threshold,
        reconciled_orders,
        unmatched_orders,
        amount_mismatches,
        orphaned_refunds,
        {{ safe_divide('reconciled_orders', 'total_orders') }} as reconciliation_rate
    from monthly
    order by order_month
)

select * from final