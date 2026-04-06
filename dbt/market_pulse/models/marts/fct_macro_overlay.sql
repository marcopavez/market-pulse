-- Daily price and return data enriched with macro indicators.
-- Macro values are forward-filled: for each price date, the most recent
-- available observation per series is used (handles monthly/quarterly cadence).
-- All macro columns will be NULL until the FRED ingestion task is active.
with prices as (
    select
        date,
        ticker,
        close,
        daily_return,
        daily_return_pct,
        volume
    from {{ ref('fct_daily_returns') }}
),

macro as (
    select * from {{ ref('stg_macro') }}
)

select
    p.date,
    p.ticker,
    p.close,
    p.daily_return,
    p.daily_return_pct,
    p.volume,

    -- Forward-filled macro indicators via lateral subqueries
    fed.value   as fed_funds_rate,
    cpi.value   as cpi,
    yc.value    as yield_curve_spread,
    ur.value    as unemployment_rate,
    gdp.value   as gdp,
    t10.value   as treasury_10y_yield

from prices p

left join lateral (
    select value from macro
    where series_id = 'FEDFUNDS' and date <= p.date
    order by date desc limit 1
) fed on true

left join lateral (
    select value from macro
    where series_id = 'CPIAUCSL' and date <= p.date
    order by date desc limit 1
) cpi on true

left join lateral (
    select value from macro
    where series_id = 'T10Y2Y' and date <= p.date
    order by date desc limit 1
) yc on true

left join lateral (
    select value from macro
    where series_id = 'UNRATE' and date <= p.date
    order by date desc limit 1
) ur on true

left join lateral (
    select value from macro
    where series_id = 'GDP' and date <= p.date
    order by date desc limit 1
) gdp on true

left join lateral (
    select value from macro
    where series_id = 'DGS10' and date <= p.date
    order by date desc limit 1
) t10 on true
