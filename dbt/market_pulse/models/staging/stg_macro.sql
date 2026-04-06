-- Depends on raw.raw_macro, which is populated by the FRED ingestion task.
-- Add that task to the Airflow DAG before running this model.
with source as (
    select * from {{ source('raw', 'raw_macro') }}
)

select
    date::date              as date,
    series_id::varchar      as series_id,
    metric_name::varchar    as metric_name,
    value::numeric(18, 6)   as value,
    extracted_at::timestamp as extracted_at
from source
where date       is not null
  and series_id  is not null
  and value      is not null
