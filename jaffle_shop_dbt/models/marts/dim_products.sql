with products as (
    select * from {{ ref('stg_products') }}
),

supplies as (
    select * from {{ ref('stg_supplies') }}
),

product_supplies as (
    select
        product_sku,
        count(distinct supply_id) as supply_count,
        sum(supply_cost_usd) as total_supply_cost,
        max(case when is_perishable then 1 else 0 end) as has_perishable_supplies
    from supplies
    group by product_sku
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

product_sales as (
    select
        product_sku,
        count(order_item_id) as total_items_sold,
        count(distinct order_id) as total_orders_with_product
    from order_items
    group by product_sku
)

select
    -- Product attributes
    p.product_sku,
    p.product_name,
    p.product_category,
    p.product_description,
    
    -- Pricing
    p.product_price_cents,
    p.product_price_usd,
    
    -- Supply chain metrics
    coalesce(ps.supply_count, 0) as supply_count,
    coalesce(ps.total_supply_cost, 0) as total_supply_cost_usd,
    coalesce(ps.has_perishable_supplies, 0) as has_perishable_supplies,
    
    -- Calculate profit margin
    p.product_price_usd - coalesce(ps.total_supply_cost, 0) as gross_profit_usd,
    case
        when p.product_price_usd > 0 then
            round((p.product_price_usd - coalesce(ps.total_supply_cost, 0)) / p.product_price_usd * 100, 2)
        else 0
    end as gross_profit_margin_pct,
    
    -- Sales metrics
    coalesce(sale.total_items_sold, 0) as total_items_sold,
    coalesce(sale.total_orders_with_product, 0) as total_orders_with_product,
    
    -- Product flags
    case when ps.has_perishable_supplies = 1 then true else false end as requires_perishable_supplies,
    case when sale.total_items_sold > 0 then true else false end as has_been_sold,
    
    -- Metadata
    current_timestamp() as dbt_updated_at
    
from products p
left join product_supplies ps on p.product_sku = ps.product_sku
left join product_sales sale on p.product_sku = sale.product_sku