with stores as (
    select * from {{ ref('stg_stores') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

store_orders as (
    select
        store_id,
        count(order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(order_total_usd) as total_revenue,
        avg(order_total_usd) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by store_id
)

select
    -- Store attributes
    s.store_id,
    s.store_name,
    s.tax_rate,
    s.store_opened_at,
    s.store_opened_date,
    s.store_age_days,
    s.store_age_years,
    
    -- Performance metrics
    coalesce(so.total_orders, 0) as total_orders,
    coalesce(so.unique_customers, 0) as unique_customers,
    coalesce(so.total_revenue, 0) as total_revenue,
    coalesce(so.avg_order_value, 0) as avg_order_value,
    
    -- Activity dates
    so.first_order_date,
    so.last_order_date,
    
    -- Derived metrics
    case
        when so.total_orders > 0 then
            round(so.total_orders::decimal / nullif(s.store_age_days, 0), 2)
        else 0
    end as avg_orders_per_day,
    
    case
        when so.total_orders > 0 then
            round(so.unique_customers::decimal / so.total_orders, 2)
        else 0
    end as customer_order_ratio,
    
    -- Store status
    case
        when so.total_orders > 0 then 'Active'
        else 'Inactive'
    end as store_status,
    
    -- Metadata
    current_timestamp() as dbt_updated_at
    
from stores s
left join store_orders so on s.store_id = so.store_id