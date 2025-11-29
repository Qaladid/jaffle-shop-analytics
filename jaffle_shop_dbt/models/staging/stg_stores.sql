with source as (
    select * from {{ source('jaffle_shop', 'raw_stores') }}
),

renamed as (
    select
        -- IDs
        id as store_id,
        
        -- Store attributes
        name as store_name,

        -- location/tax data
        tax_rate,

        -- metadata
        opened_at as store_opened_at,
        to_date(opened_at) as store_opened_date,

        -- calculate stoer age
        datediff('day', to_date(opened_at), current_date()) as store_age_days,
        datediff('year', to_date(opened_at), current_date()) as store_age_years,
        
    from source
)

select * from renamed