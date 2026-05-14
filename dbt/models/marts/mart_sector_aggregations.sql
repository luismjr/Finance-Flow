-- Mart model: mart_sector_aggregations
-- Aggregates daily returns, volatility, and volume at the sector level.
-- Identifies daily top and bottom performers per sector.

with analytics as (
    select * from {{ ref('mart_stock_analytics') }}
),

vol_metrics as (
    select ticker, date, annualized_vol
    from {{ ref('mart_volatility_metrics') }}
),

company_info as (
    select ticker, sector
    from {{ ref('dim_company_info') }}
),

joined as (
    select
        a.ticker,
        a.date,
        a.daily_return,
        a.close_price,
        a.volume,
        v.annualized_vol,
        c.sector

    from analytics a
    join company_info c using (ticker)
    left join vol_metrics v using (ticker, date)

    where c.sector is not null
),

-- Rank tickers within each sector/date for top/bottom performers
ranked as (
    select
        *,
        row_number() over (
            partition by sector, date order by daily_return desc
        ) as rank_top,
        row_number() over (
            partition by sector, date order by daily_return asc
        ) as rank_bottom
    from joined
    where daily_return is not null
),

aggregated as (
    select
        sector,
        date,
        avg(daily_return)       as avg_return,
        avg(annualized_vol)     as avg_volatility,
        sum(volume)             as total_volume,
        count(distinct ticker)  as num_stocks,
        max(case when rank_top    = 1 then ticker end) as top_performer,
        max(case when rank_bottom = 1 then ticker end) as bottom_performer,
        current_timestamp       as computed_at

    from ranked
    group by sector, date
)

select * from aggregated
