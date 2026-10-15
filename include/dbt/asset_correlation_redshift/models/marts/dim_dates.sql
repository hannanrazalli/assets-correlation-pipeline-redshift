{{ config(
    materialized='table',
    sort='price_date',
    dist='ALL' 
) }}

with digits as (
    select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
    union all select 5 union all select 6 union all select 7 union all select 8 union all select 9
),

numbers as (
    select (d3.n * 1000 + d2.n * 100 + d1.n * 10 + d0.n) as num
    from digits d0
    cross join digits d1
    cross join digits d2
    cross join digits d3
),

date_spine as (
    select dateadd(day, num, '2020-01-01')::date as date_val
    from numbers
    where dateadd(day, num, '2020-01-01') <= '2030-12-31'
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