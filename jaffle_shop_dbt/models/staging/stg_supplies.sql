with source as (
    select * from {{ source('jaffle_shop', 'raw_supplies') }}
),

renamed as (
    select
        -- IDs
        id as supply_id,
        sku as product_sku,

        -- supply attributes
        name as supply_name,

        -- Costs (in cents)
        cost as supply_cost_cents,

        -- convert cost to dollars
        round(cost / 100.0, 4) as supply_cost_usd,
        
        -- Flags
        perishable as is_perishable

    from source
)

select * from renamed