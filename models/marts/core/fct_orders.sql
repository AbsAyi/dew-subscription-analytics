with orders as (
    select * from {{ ref('int_orders__enriched') }}
),

customers as (
    select customer_id, acquisition_cohort_month, acquisition_channel
    from {{ ref('dim_customers') }}
),

final as (
    select
        o.order_id,
        o.order_number,
        o.shopify_customer_id as customer_id,
        date(o.order_created_at_utc) as order_date,
        o.order_month,
        c.acquisition_cohort_month,
        c.acquisition_channel,
        o.customer_order_sequence,
        case when o.customer_order_sequence = 1 then true else false end as is_first_order,
        o.gross_revenue,
        o.discount_amount,
        o.net_revenue,
        o.tax_amount,
        o.shipping_amount,
        o.item_count,
        o.total_units,
        o.product_names,
        o.skus,
        o.is_subscription_order,
        o.financial_status,
        o.fulfillment_status,
        o.discount_codes,
        o.billing_state,
        o.utm_source,
        o.utm_medium,
        o.channel
    from orders o
    left join customers c on o.shopify_customer_id = c.customer_id
)

select * from final