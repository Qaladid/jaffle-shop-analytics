with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

order_items_enriched as (
    select
        oi.order_id,
        count(oi.order_item_id) as item_count,
        count(distinct oi.product_sku) as unique_products,
        sum(p.product_price_usd) as calculated_subtotal
    from order_items oi
    left join products p on oi.product_sku = p.product_sku
    group by oi.order_id
)

select
    -- Order identifiers
    o.order_id,
    o.customer_id,
    o.store_id,
    
    -- Date dimensions
    o.ordered_at,
    o.order_date,
    o.order_month,
    o.order_year,
    o.order_quarter,
    o.order_day_of_week,
    o.order_day_name,
    o.order_hour,
    
    -- Financial metrics (in USD)
    o.subtotal_usd,
    o.tax_paid_usd,
    o.order_total_usd,
    o.tax_rate_applied,
    
    -- Order composition
    coalesce(oie.item_count, 0) as item_count,
    coalesce(oie.unique_products, 0) as unique_products,
    
    -- Derived metrics
    case
        when oie.item_count > 0 then
            round(o.order_total_usd / oie.item_count, 2)
        else 0
    end as avg_price_per_item,
    
    -- Order flags
    case when oie.item_count >= 5 then true else false end as is_large_order,
    case when o.order_hour >= 11 and o.order_hour <= 14 then true else false end as is_lunch_order,
    case when o.order_day_of_week in (0, 6) then true else false end as is_weekend_order,
    
    -- Metadata
    current_timestamp() as dbt_updated_at
    
from orders o
left join order_items_enriched oie on o.order_id = oie.order_id