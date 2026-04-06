-- Rolling 30-day pairwise return correlations.
-- Grain: one row per (date, ticker_a, ticker_b) where ticker_a < ticker_b.
-- Rows with fewer than 30 observations in the window are excluded.
-- With 5 tickers there are C(5,2) = 10 pairs.
with returns as (
    select
        date,
        ticker,
        daily_return
    from {{ ref('fct_daily_returns') }}
),

pairs as (
    select
        a.date,
        a.ticker       as ticker_a,
        b.ticker       as ticker_b,
        a.daily_return as return_a,
        b.daily_return as return_b
    from returns a
    inner join returns b
        on  a.date   = b.date
        and a.ticker < b.ticker   -- deduplicate: only store (A,B), not (B,A)
),

rolling as (
    select
        date,
        ticker_a,
        ticker_b,
        corr(return_a, return_b) over (
            partition by ticker_a, ticker_b
            order by date
            rows between 29 preceding and current row
        )                                           as correlation_30d,
        count(*) over (
            partition by ticker_a, ticker_b
            order by date
            rows between 29 preceding and current row
        )                                           as obs_count
    from pairs
)

select
    date,
    ticker_a,
    ticker_b,
    round(correlation_30d::numeric, 4)  as correlation_30d,
    obs_count
from rolling
where obs_count = 30
order by date, ticker_a, ticker_b
