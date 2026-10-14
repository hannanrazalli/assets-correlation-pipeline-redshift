-- depends_on: {{ ref('int_assets') }}

{{ config(
    materialized='table',
    sort='price_date',
    dist='ALL' 
) }}

with date_spine as (
    select 
        dateadd(day, row_number() over (order by true) - 1, '2020-01-01')::date as date_val
    from 
        pg_catalog.svv_table_info 
    limit 4000 
)

select
    date_val as price_date,
    extract(year from date_val) as year,
    extract(quarter from date_val) as quarter,
    extract(month from date_val) as month,
    to_char(date_val, 'Month') as month_name,
    extract(day from date_val) as day,
    extract(dow from date_val) as day_of_week,
    to_char(date_val, 'Day') as day_name,
    case when extract(dow from date_val) in (0, 6) then true else false end as is_weekend
from date_spine
where date_val <= '2030-12-31'