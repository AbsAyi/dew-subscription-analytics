/*
    Monthly subscription KPIs: MRR, active subs, churn, payment failures.
    Grain: One row per month.
*/
with subscriptions as (
    select * from {{ ref('fct_subscriptions') }}
),

charges as (
    select * from {{ ref('fct_charges') }}
),

date_spine as (
    select distinct month_start as report_month
    from {{ ref('dim_dates') }}
    where full_date between '2024-01-01' and '2025-12-31'
),

monthly_subs as (
    select
        ds.report_month,
        count(distinct case
            when s.subscription_created_at_utc <= dateadd('month', 1, ds.report_month)
                 and (s.subscription_cancelled_at_utc is null
                      or s.subscription_cancelled_at_utc > ds.report_month)
            then s.subscription_id
        end) as active_subscriptions,
        count(distinct case
            when date_trunc('month', s.subscription_created_at_utc) = ds.report_month
            then s.subscription_id
        end) as new_subscriptions,
        count(distinct case
            when date_trunc('month', s.subscription_cancelled_at_utc) = ds.report_month
            then s.subscription_id
        end) as cancelled_subscriptions,
        coalesce(sum(case
            when s.subscription_created_at_utc <= dateadd('month', 1, ds.report_month)
                 and (s.subscription_cancelled_at_utc is null
                      or s.subscription_cancelled_at_utc > ds.report_month)
            then s.monthly_recurring_revenue else 0
        end), 0) as monthly_recurring_revenue
    from date_spine ds
    cross join subscriptions s
    group by ds.report_month
),

monthly_charges as (
    select
        charge_month as report_month,
        count(*) as total_charge_attempts,
        sum(case when is_successful_charge then 1 else 0 end) as successful_charges,
        sum(case when is_failed_charge then 1 else 0 end) as failed_charges,
        sum(case when is_successful_charge then charge_total else 0 end) as charged_revenue
    from charges
    group by charge_month
),

final as (
    select
        ms.report_month,
        ms.active_subscriptions,
        ms.new_subscriptions,
        ms.cancelled_subscriptions,
        ms.monthly_recurring_revenue,
        {{ safe_divide('ms.cancelled_subscriptions', 'ms.active_subscriptions') }} as churn_rate,
        ms.new_subscriptions - ms.cancelled_subscriptions as net_subscription_change,
        coalesce(mc.total_charge_attempts, 0) as total_charge_attempts,
        coalesce(mc.successful_charges, 0) as successful_charges,
        coalesce(mc.failed_charges, 0) as failed_charges,
        {{ safe_divide('mc.failed_charges', 'mc.total_charge_attempts') }} as payment_failure_rate,
        coalesce(mc.charged_revenue, 0) as charged_revenue
    from monthly_subs ms
    left join monthly_charges mc on ms.report_month = mc.report_month
    order by ms.report_month
)

select * from final