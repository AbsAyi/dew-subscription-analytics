with charges as (
    select * from {{ ref('stg_recharge__charges') }}
),

final as (
    select
        charge_id,
        subscription_id,
        recharge_customer_id,
        shopify_order_id,
        customer_email,
        charge_created_at_utc,
        date(charge_created_at_utc) as charge_date,
        date_trunc('month', charge_created_at_utc) as charge_month,
        charge_type,
        charge_status,
        is_successful_charge,
        is_failed_charge,
        charge_total,
        charge_subtotal,
        charge_tax,
        discount_codes,
        error_message,
        error_category,
        payment_processor
    from charges
)

select * from final