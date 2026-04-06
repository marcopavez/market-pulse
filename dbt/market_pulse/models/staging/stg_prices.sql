with source as (
    select * from {{ source('raw', 'raw_prices') }}
)

select
    date::date                                      as date,
    upper(trim(ticker))::varchar(20)                as ticker,
    open::numeric(18, 6)                            as open,
    high::numeric(18, 6)                            as high,
    low::numeric(18, 6)                             as low,
    close::numeric(18, 6)                           as close,
    volume::bigint                                  as volume,
    coalesce(dividends, 0)::numeric(18, 6)          as dividends,
    coalesce(stock_splits, 0)::numeric(18, 6)       as stock_splits,
    extracted_at::timestamp                         as extracted_at
from source
where date       is not null
  and ticker     is not null
  and close      >  0
  and high       >= low
