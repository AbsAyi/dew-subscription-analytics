/*
    Unified customer spine. Joins Shopify + Recharge on email.
    Grain: One row per customer.
*/
with shopify_customers as (
    select * from {{ ref('stg_shopify__customers') }}
),

recharge_customers as (
    select
        *,
        row_number() over (
            partition by customer_email
            order by recharge_created_at_utc desc
        ) as row_num
    from {{ ref('stg_recharge__customers') }}
),

unified as (
    select
        sc.shopify_customer_id,
        rc.recharge_customer_id,
        sc.customer_email,
        sc.first_name,
        sc.last_name,
        sc.billing_state,
        sc.customer_created_at_utc,
        sc.shopify_orders_count,
        sc.shopify_total_spent,
        sc.accepts_marketing,
        case
            when rc.recharge_customer_id is not null then true
            else false
        end as is_subscriber,
        rc.recharge_status,
        rc.is_in_dunning
    from shopify_customers sc
    left join recharge_customers rc
        on sc.customer_email = rc.customer_email
        and rc.row_num = 1
)

select * from unified