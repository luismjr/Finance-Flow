-- Staging model: stg_macro_indicators
-- Cleans FRED observations: drops null values, standardises types.

with source as (
    select * from {{ source('raw', 'pipeline_macroindicator') }}
),

cleaned as (
    select
        series_id::varchar(50)                      as series_id,
        name::varchar(200)                          as name,
        date::date                                  as date,
        value::numeric(20, 6)                       as value,
        unit::varchar(50)                           as unit,
        frequency::varchar(20)                      as frequency,
        ingested_at::timestamp with time zone       as ingested_at

    from source

    where
        value is not null
        and series_id is not null
        and date is not null
)

select * from cleaned
