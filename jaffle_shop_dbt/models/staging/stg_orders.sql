with source as (
    select *
    from {{ source('jaffle_shop', 'raw_orders') }}
),

renamed as (
    select
        id as order_id,
        customer as customer_id,
        ordered_at::timestamp as ordered_at,
        store_id,
        subtotal::number as subtotal,
        tax_paid::number as tax_paid,
        order_total::number as order_total
    from source
)

select * from renamed;
