with customers as (
    select * from {{ ref('int_customers__unified') }}
),

orders as (
    select * from {{ ref('int_orders__enriched') }}
),

-- First-touch attribution (separate CTE to avoid window func in GROUP BY)
first_touch as (
    select distinct
        shopify_customer_id,
        first_value(channel) ignore nulls over (
            partition by shopify_customer_id
            order by order_created_at_utc
        ) as acquisition_channel
    from orders
),

customer_orders as (
    select
        shopify_customer_id,
        count(*) as lifetime_orders,
        sum(net_revenue) as lifetime_revenue,
        min(order_created_at_utc) as first_order_at,
        max(order_created_at_utc) as last_order_at,
        date_trunc('month', min(order_created_at_utc)) as acquisition_cohort_month,
        avg(net_revenue) as avg_order_value
    from orders
    group by shopify_customer_id
),

subscriptions as (
    select
        shopify_customer_id,
        count(*) as total_subscriptions,
        sum(case when subscription_status = 'active' then 1 else 0 end) as active_subscriptions,
        max(case when subscription_status = 'active' then subscription_price else null end)
            as current_subscription_price
    from {{ ref('stg_recharge__subscriptions') }}
    group by shopify_customer_id
),

final as (
    select
        c.shopify_customer_id as customer_id,
        c.customer_email,
        c.first_name,
        c.last_name,
        c.billing_state,
        c.customer_created_at_utc,
        c.accepts_marketing,

        coalesce(co.lifetime_orders, 0) as lifetime_orders,
        coalesce(co.lifetime_revenue, 0) as lifetime_revenue,
        co.first_order_at,
        co.last_order_at,
        co.acquisition_cohort_month,
        co.avg_order_value,
        ft.acquisition_channel,

        case
            when c.is_subscriber then 'subscriber'
            else 'one_time'
        end as customer_type,

        coalesce(s.total_subscriptions, 0) as total_subscriptions,
        coalesce(s.active_subscriptions, 0) as active_subscriptions,
        s.current_subscription_price,
        c.is_in_dunning,

        datediff('day', co.last_order_at, current_timestamp()) as days_since_last_order

    from customers c
    left join customer_orders co on c.shopify_customer_id = co.shopify_customer_id
    left join first_touch ft on c.shopify_customer_id = ft.shopify_customer_id
    left join subscriptions s on c.shopify_customer_id = s.shopify_customer_id
)

select * from final