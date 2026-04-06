-- Rolling 30-day annualized volatility per ticker.
-- Rows with fewer than 30 observations in the window are excluded.
-- Annualization factor: sqrt(252) trading days per year.
with returns as (
    select
        date,
        ticker,
        daily_return
    from {{ ref('fct_daily_returns') }}
),

windowed as (
    select
        date,
        ticker,
        stddev(daily_return) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        )                                           as rolling_stddev,
        count(*) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        )                                           as obs_count
    from returns
)

select
    date,
    ticker,
    round((rolling_stddev * sqrt(252) * 100)::numeric, 4)  as volatility_30d_annualized_pct,
    round((rolling_stddev * 100)::numeric, 6)              as volatility_30d_daily_pct,
    obs_count
from windowed
where obs_count = 30
