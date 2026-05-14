-- Staging model: stg_stock_prices
-- Cleans and type-casts raw OHLCV data before mart transformations.
-- Filters out any rows with null prices or zero volume (data quality guard).

with source as (
    select * from {{ source('raw', 'pipeline_stockprice') }}
),

cleaned as (
    select
        ticker::varchar(10)                         as ticker,
        date::date                                  as date,
        open_price::numeric(12, 4)                  as open_price,
        high_price::numeric(12, 4)                  as high_price,
        low_price::numeric(12, 4)                   as low_price,
        close_price::numeric(12, 4)                 as close_price,
        adj_close::numeric(12, 4)                   as adj_close,
        volume::bigint                              as volume,
        ingested_at::timestamp with time zone       as ingested_at,
        source::varchar(50)                         as source

    from source

    where
        close_price is not null
        and close_price > 0
        and open_price  > 0
        and high_price  > 0
        and low_price   > 0
        and volume      > 0
        and high_price >= low_price          -- sanity check: high must be ≥ low
)

select * from cleaned
