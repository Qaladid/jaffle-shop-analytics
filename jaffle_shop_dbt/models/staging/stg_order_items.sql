with source as (
    select * from {{ source('jaffle_shop', 'raw_items') }}
),

renamed as (
    select
        -- IDs
        id as order_item_id,
        order_id,
        sku as product_sku
        
        -- Note: No quantity or price in raw_items
        -- We'll join with products in the marts layer to get pricing
        
    from source
)

select * from renamed