with subscriptions as (
    select * from {{ ref('stg_recharge__subscriptions') }}
),

charges as (
    select
        subscription_id,
        count(*) as total_charge_attempts,
        sum(case when is_successful_charge then 1 else 0 end) as successful_charges,
        sum(case when is_failed_charge then 1 else 0 end) as failed_charges,
        sum(case when is_successful_charge then charge_total else 0 end) as total_charged_revenue
    from {{ ref('stg_recharge__charges') }}
    group by subscription_id
),

final as (
    select
        s.subscription_id,
        s.shopify_customer_id as customer_id,
        s.customer_email,
        s.product_title,
        s.variant_title,
        s.sku,
        s.subscription_price,
        s.subscription_frequency_tier,
        s.charge_interval_days,
        s.subscription_status,
        s.subscription_created_at_utc,
        s.subscription_cancelled_at_utc,
        s.cancellation_reason,
        s.had_status_mismatch,

        case
            when s.subscription_cancelled_at_utc is not null
            then datediff('day', s.subscription_created_at_utc, s.subscription_cancelled_at_utc)
            else datediff('day', s.subscription_created_at_utc, current_timestamp())
        end as subscription_duration_days,

        coalesce(c.total_charge_attempts, 0) as total_charge_attempts,
        coalesce(c.successful_charges, 0) as successful_charges,
        coalesce(c.failed_charges, 0) as failed_charges,
        coalesce(c.total_charged_revenue, 0) as total_charged_revenue,

        {{ safe_divide('c.failed_charges', 'c.total_charge_attempts') }} as failure_rate,

        case
            when s.subscription_status = 'active'
            then round(s.subscription_price * (30.0 / s.charge_interval_days), 2)
            else 0
        end as monthly_recurring_revenue

    from subscriptions s
    left join charges c on s.subscription_id = c.subscription_id
)

select * from final