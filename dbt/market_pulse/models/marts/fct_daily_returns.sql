with prices as (
    select * from {{ ref('stg_prices') }}
),

with_lag as (
    select
        date,
        ticker,
        close,
        volume,
        lag(close) over (partition by ticker order by date) as prev_close,
        extracted_at
    from prices
)

select
    date,
    ticker,
    close,
    prev_close,
    round(
        ((close - prev_close) / nullif(prev_close, 0))::numeric,
        8
    )                                                           as daily_return,
    round(
        (((close - prev_close) / nullif(prev_close, 0)) * 100)::numeric,
        4
    )                                                           as daily_return_pct,
    volume,
    extracted_at
from with_lag
where prev_close is not null
