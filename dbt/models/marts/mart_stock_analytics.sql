-- Mart model: mart_stock_analytics
-- Builds 7-, 30-, and 90-day moving averages and daily returns per ticker.
-- Materialized as a table; refreshed daily by the dbt_transform DAG.

with base as (
    select
        ticker,
        date,
        close_price,
        volume,
        -- Daily log return
        ln(close_price / lag(close_price) over (
            partition by ticker order by date
        )) as daily_return

    from {{ ref('stg_stock_prices') }}
),

with_moving_averages as (
    select
        ticker,
        date,
        close_price,
        volume,
        daily_return,

        -- Simple moving averages (rolling window, min 1 period to avoid nulls at series start)
        avg(close_price) over (
            partition by ticker
            order by date
            rows between 6 preceding and current row
        ) as ma_7d,

        avg(close_price) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ) as ma_30d,

        avg(close_price) over (
            partition by ticker
            order by date
            rows between 89 preceding and current row
        ) as ma_90d,

        -- Record metadata
        current_timestamp as computed_at

    from base
)

select * from with_moving_averages
