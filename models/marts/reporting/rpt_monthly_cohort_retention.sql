/*
    Cohort retention: what % of each monthly cohort is still ordering N months later.
    Powers the retention heatmap in Power BI.
    Grain: One row per cohort_month x months_since_first_order.
*/
with customer_active_months as (
    select distinct
        customer_id,
        acquisition_cohort_month,
        order_month
    from {{ ref('fct_orders') }}
),

with_period as (
    select
        customer_id,
        acquisition_cohort_month,
        order_month,
        datediff('month', acquisition_cohort_month, order_month) as months_since_first_order
    from customer_active_months
),

cohort_sizes as (
    select
        acquisition_cohort_month as cohort_month,
        count(distinct customer_id) as cohort_size
    from with_period
    where months_since_first_order = 0
    group by acquisition_cohort_month
),

retention as (
    select
        acquisition_cohort_month as cohort_month,
        months_since_first_order,
        count(distinct customer_id) as customers_active
    from with_period
    group by acquisition_cohort_month, months_since_first_order
),

final as (
    select
        r.cohort_month,
        r.months_since_first_order,
        cs.cohort_size,
        r.customers_active,
        round(r.customers_active::float / cs.cohort_size, 4) as retention_rate
    from retention r
    inner join cohort_sizes cs on r.cohort_month = cs.cohort_month
    order by r.cohort_month, r.months_since_first_order
)

select * from final