with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(order_id) as total_orders,
        sum(order_total_usd) as lifetime_value,
        avg(order_total_usd) as avg_order_value,
        sum(subtotal_usd) as total_spent_pre_tax,
        sum(tax_paid_usd) as total_tax_paid
    from orders 
    group by customer_id
)

select 
    -- customer attributes
    c.customer_id,
    c.customer_name,

    -- Order metrics
    co.first_order_date,
    co.last_order_date,
    co.total_orders,

    -- Financial metrics
    co.lifetime_value,
    co.avg_order_value,
    co.total_spent_pre_tax,
    co.total_tax_paid,

    -- Derived metrics
    datediff('day', co.first_order_date, co.last_order_date) as customer_lifetime_days,
    datediff('day', co.last_order_date, current_date()) as days_since_last_order,

    -- Customer segmentation
    case
        when co.total_orders >= 10 then 'High Frequency'
        when co.total_orders >= 5 then 'Medium Frequency'
        else 'Low Frequency'
    end as order_frequency_segment,

    case
        when co.lifetime_value >= 1000 then 'High Value'
        when co.lifetime_value >= 500 then 'Medium Value'
        else 'Low Value'
    end as customer_value_segment,

    -- Status flags
    case
        when datediff('day', co.last_order_date, current_date()) > 180 then true
        else false
    end as is_churned,

    -- Metadata
    current_timestamp() as dbt_updated_at

from customers c
left join customer_orders co on c.customer_id = co.customer_id