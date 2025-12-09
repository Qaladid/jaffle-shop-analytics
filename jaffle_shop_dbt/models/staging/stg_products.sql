with source as (
    select * from {{ source('jaffle_shop', 'raw_products') }}
    where sku is not null
),

renamed as (
    select
        -- IDs
        sku as product_sku,
        
        -- Product attributes
        name as product_name,
        description as product_description,

        -- Pricing (in cents)
        coalesce(price, 0) as product_price_cents,
              
        -- Convert to dollars
        round(coalesce(price, 0) / 100.0, 1) as product_price_usd,
        
        -- Categorization (clean up type field)
        case
            when lower(type) = 'jaffle' then 'Jaffle'
            when lower(type) = 'beverage' then 'Beverage'
            else initcap(type)
        end as product_category
        
    from source
)

select * from renamed