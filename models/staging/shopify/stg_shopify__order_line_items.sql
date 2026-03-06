/*
    stg_shopify__order_line_items
    
    Clean and standardize order line item data.
    
    Grain: One row per line item
*/

with source as (
    select * from {{ source('shopify', 'raw_shopify__order_line_items') }}
),

-- Deduplicate line items that came in with duplicate orders
deduplicated as (
    select
        *,
        row_number() over (
            partition by id 
            order by id
        ) as row_num
    from source
),

cleaned as (
    select
        id                                          as line_item_id,
        order_id,
        product_id,
        title                                       as product_title,
        variant_title,
        sku,
        quantity::integer                            as quantity,
        price::decimal(10,2)                        as unit_price,
        total_discount::decimal(10,2)               as line_discount_amount,
        (price::decimal(10,2) * quantity::integer)  as gross_line_amount,
        (price::decimal(10,2) * quantity::integer 
         - total_discount::decimal(10,2))           as net_line_amount

    from deduplicated
    where row_num = 1
)

select * from cleaned
