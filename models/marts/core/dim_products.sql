with products as (
    select distinct
        product_id,
        product_title,
        variant_title,
        sku,
        unit_price
    from {{ ref('stg_shopify__order_line_items') }}
),

enriched as (
    select
        product_id,
        product_title,
        variant_title,
        sku,
        unit_price as current_price,
        case
            when variant_title ilike '%30-day%' then 30
            when variant_title ilike '%90-day%' then 90
            else null
        end as supply_duration_days,
        case
            when variant_title ilike '%30-day%' then '30-day'
            when variant_title ilike '%90-day%' then '90-day'
            else 'other'
        end as supply_tier,
        case
            when product_title ilike '%bundle%' then 'bundle'
            else 'single'
        end as product_type
    from products
)

select * from enriched