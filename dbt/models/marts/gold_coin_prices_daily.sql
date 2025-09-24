
with s as (
    select
        date_trunc('day', ingestion_ts) as day,
        coin_id, symbol, name,
        avg(current_price) as avg_price_usd,
        avg(market_cap) as avg_market_cap,
        avg(total_volume) as avg_total_volume
    from {{ ref('stg_coin_market') }}
    group by 1,2,3,4
)
select * from s
