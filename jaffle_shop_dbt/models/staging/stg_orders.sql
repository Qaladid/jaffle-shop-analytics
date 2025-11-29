with source as (
    select * from {{ source('jaffle_shop', 'raw_orders') }}
),

renamed as (
    select
        -- IDs
        id as order_id,
        customer as customer_id,  -- Note: column is named 'customer' not 'customer_id'
        store_id,
        
        -- Dates
        ordered_at,
        to_date(ordered_at) as order_date,
        
        -- Extract date parts for analysis
        date_trunc('month', ordered_at) as order_month,
        date_trunc('year', ordered_at) as order_year,
        date_trunc('quarter', ordered_at) as order_quarter,
        dayofweek(ordered_at) as order_day_of_week,
        dayname(ordered_at) as order_day_name,
        hour(ordered_at) as order_hour,
        
        -- Financial metrics (in cents)
        subtotal,
        tax_paid,
        order_total,
        
        -- Derived metrics
        round(tax_paid::decimal / nullif(subtotal, 0), 4) as tax_rate_applied,
        
        -- Convert to dollars (if preferred)
        subtotal / 100.0 as subtotal_usd,
        tax_paid / 100.0 as tax_paid_usd,
        order_total / 100.0 as order_total_usd
        
    from source
)

select * from renamed