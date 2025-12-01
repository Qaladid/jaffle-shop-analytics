with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    -- Item identifiers
    oi.order_item_id,
    oi.order_id,
    oi.product_sku,
    
    -- Foreign keys
    o.customer_id,
    o.store_id,
    
    -- Date dimensions (denormalized from order)
    o.order_date,
    o.order_month,
    o.order_year,
    o.ordered_at,
    
    -- Product attributes (denormalized)
    p.product_name,
    p.product_category,
    
    -- Financial metrics
    p.product_price_usd as item_price_usd,
    
    -- Note: Since raw_items doesn't have quantity, we assume quantity = 1
    1 as quantity,
    p.product_price_usd as line_total_usd,
    
    -- Metadata
    current_timestamp() as dbt_updated_at
    
from order_items oi
inner join orders o on oi.order_id = o.order_id
inner join products p on oi.product_sku = p.product_sku