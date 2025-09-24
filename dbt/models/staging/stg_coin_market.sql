
with base as (
    select
        ingestion_ts,
        coin_id,
        symbol,
        name,
        cast(current_price as numeric) as current_price,
        cast(market_cap as numeric) as market_cap,
        cast(total_volume as numeric) as total_volume
    from {{ source('raw_src','coin_market') }}
)
select * from base
