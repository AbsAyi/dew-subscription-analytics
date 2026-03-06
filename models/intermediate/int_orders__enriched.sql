/*
    Orders enriched with line items and order sequence number.
    The sequence number is critical for cohort analysis.
    Grain: One row per order.
*/
with orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

line_items as (
    select * from {{ ref('stg_shopify__order_line_items') }}
),

order_items as (
    select
        order_id,
        count(*) as item_count,
        sum(quantity) as total_units,
        sum(net_line_amount) as line_item_revenue,
        listagg(distinct product_title, ', ')
            within group (order by product_title) as product_names,
        listagg(distinct sku, ', ')
            within group (order by sku) as skus
    from line_items
    group by order_id
),

enriched as (
    select
        o.order_id,
        o.order_number,
        o.shopify_customer_id,
        o.customer_email,
        o.order_created_at_utc,
        date_trunc('month', o.order_created_at_utc) as order_month,

        row_number() over (
            partition by o.shopify_customer_id
            order by o.order_created_at_utc
        ) as customer_order_sequence,

        o.gross_revenue,
        o.discount_amount,
        o.net_revenue,
        o.tax_amount,
        o.shipping_amount,

        coalesce(oi.item_count, 0) as item_count,
        coalesce(oi.total_units, 0) as total_units,
        oi.product_names,
        oi.skus,

        o.is_subscription_order,
        o.financial_status,
        o.fulfillment_status,
        o.discount_codes,
        o.billing_state,
        o.utm_source,
        o.utm_medium,
        case
            when o.utm_source is not null and o.utm_medium is not null
            then o.utm_source || ' / ' || o.utm_medium
            when o.utm_source is not null
            then o.utm_source || ' / (none)'
            else null
        end as channel

    from orders o
    left join order_items oi on o.order_id = oi.order_id
    where o.financial_status != 'voided'
)

select * from enriched