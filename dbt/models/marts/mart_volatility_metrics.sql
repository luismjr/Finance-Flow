-- Mart model: mart_volatility_metrics
-- Computes rolling 30- and 90-day realized volatility plus annualized vol and Sharpe ratio.
-- Beta is computed against SPY returns (if SPY is in the tracked tickers).

with analytics as (
    select * from {{ ref('mart_stock_analytics') }}
),

-- SPY benchmark for beta calculation
spy_returns as (
    select
        date,
        daily_return as spy_return
    from analytics
    where ticker = 'SPY'
),

joined as (
    select
        a.ticker,
        a.date,
        a.daily_return,
        s.spy_return
    from analytics a
    left join spy_returns s using (date)
),

volatility as (
    select
        ticker,
        date,

        -- Rolling 30-day realized volatility (population std dev of daily log returns)
        stddev_pop(daily_return) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ) as rolling_vol_30d,

        -- Rolling 90-day realized volatility
        stddev_pop(daily_return) over (
            partition by ticker
            order by date
            rows between 89 preceding and current row
        ) as rolling_vol_90d,

        -- 30-day mean return (for Sharpe numerator)
        avg(daily_return) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ) as mean_return_30d,

        -- Beta: cov(r_i, r_m) / var(r_m) over 90 days
        covar_pop(daily_return, spy_return) over (
            partition by ticker
            order by date
            rows between 89 preceding and current row
        ) / nullif(
            var_pop(spy_return) over (
                partition by ticker
                order by date
                rows between 89 preceding and current row
            ),
        0) as beta,

        current_timestamp as computed_at

    from joined
)

select
    ticker,
    date,
    rolling_vol_30d,
    rolling_vol_90d,
    -- Annualized volatility: σ_daily × √252
    rolling_vol_30d * sqrt(252)                                     as annualized_vol,
    beta,
    -- Annualized Sharpe ratio (assumes risk-free rate ≈ 0 for simplicity)
    (mean_return_30d / nullif(rolling_vol_30d, 0)) * sqrt(252)      as sharpe_30d,
    computed_at

from volatility
