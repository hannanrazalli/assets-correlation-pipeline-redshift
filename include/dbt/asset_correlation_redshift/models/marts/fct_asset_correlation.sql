with fct_daily_returns as (
    select * from {{ ref('fct_daily_returns') }}
),

returns_pivoted as (
    select
        price_date,
        max(case when ticker_symbol = '^GSPC' then daily_returns_pct end) as sp500_return,
        max(case when ticker_symbol = '^KLSE' then daily_returns_pct end) as klci_return,
        max(case when ticker_symbol = 'BTC_USD' then daily_returns_pct end) as btc_return,
        max(case when ticker_symbol = 'GLD' then daily_returns_pct end) as gold_return,
        row_number() over(order by price_date) as day_number
    from fct_daily_returns
    group by price_date
    having max(case when ticker_symbol = '^GSPC' then daily_returns_pct end) is not null
       and max(case when ticker_symbol = '^KLSE' then daily_returns_pct end) is not null
       and max(case when ticker_symbol = 'BTC_USD' then daily_returns_pct end) is not null
       and max(case when ticker_symbol = 'GLD' then daily_returns_pct end) is not null
),

correlation as (
    select
        price_date,
        day_number,

        -- 1. KLCI vs S&P 500
        round(cast(
            (
                avg(klci_return::float8 * sp500_return::float8) over(order by price_date rows between 29 preceding and current row)
                - (avg(klci_return::float8) over(order by price_date rows between 29 preceding and current row)
                   * avg(sp500_return::float8) over(order by price_date rows between 29 preceding and current row))
            )
            / nullif(
                stddev_pop(klci_return::float8) over(order by price_date rows between 29 preceding and current row)
                * stddev_pop(sp500_return::float8) over(order by price_date rows between 29 preceding and current row),
                0
            )
        as decimal(18, 6)), 2) as corr_sp500_klse_30d,

        -- 2. S&P 500 vs BTC
        round(cast(
            (
                avg(btc_return::float8 * sp500_return::float8) over(order by price_date rows between 29 preceding and current row)
                - (avg(btc_return::float8) over(order by price_date rows between 29 preceding and current row)
                   * avg(sp500_return::float8) over(order by price_date rows between 29 preceding and current row))
            )
            / nullif(
                stddev_pop(btc_return::float8) over(order by price_date rows between 29 preceding and current row)
                * stddev_pop(sp500_return::float8) over(order by price_date rows between 29 preceding and current row),
                0
            )
        as decimal(18, 6)), 2) as corr_sp500_btc_30d,

        -- 3. S&P 500 vs Gold
        round(cast(
            (
                avg(gold_return::float8 * sp500_return::float8) over(order by price_date rows between 29 preceding and current row)
                - (avg(gold_return::float8) over(order by price_date rows between 29 preceding and current row)
                   * avg(sp500_return::float8) over(order by price_date rows between 29 preceding and current row))
            )
            / nullif(
                stddev_pop(gold_return::float8) over(order by price_date rows between 29 preceding and current row)
                * stddev_pop(sp500_return::float8) over(order by price_date rows between 29 preceding and current row),
                0
            )
        as decimal(18, 6)), 2) as corr_sp500_gold_30d,

        -- 4. KLCI vs BTC
        round(cast(
            (
                avg(btc_return::float8 * klci_return::float8) over(order by price_date rows between 29 preceding and current row)
                - (avg(btc_return::float8) over(order by price_date rows between 29 preceding and current row)
                   * avg(klci_return::float8) over(order by price_date rows between 29 preceding and current row))
            )
            / nullif(
                stddev_pop(btc_return::float8) over(order by price_date rows between 29 preceding and current row)
                * stddev_pop(klci_return::float8) over(order by price_date rows between 29 preceding and current row),
                0
            )
        as decimal(18, 6)), 2) as corr_klse_btc_30d,

        -- 5. KLCI vs Gold
        round(cast(
            (
                avg(gold_return::float8 * klci_return::float8) over(order by price_date rows between 29 preceding and current row)
                - (avg(gold_return::float8) over(order by price_date rows between 29 preceding and current row)
                   * avg(klci_return::float8) over(order by price_date rows between 29 preceding and current row))
            )
            / nullif(
                stddev_pop(gold_return::float8) over(order by price_date rows between 29 preceding and current row)
                * stddev_pop(klci_return::float8) over(order by price_date rows between 29 preceding and current row),
                0
            )
        as decimal(18, 6)), 2) as corr_klse_gold_30d,

        -- 6. BTC vs Gold
        round(cast(
            (
                avg(gold_return::float8 * btc_return::float8) over(order by price_date rows between 29 preceding and current row)
                - (avg(gold_return::float8) over(order by price_date rows between 29 preceding and current row)
                   * avg(btc_return::float8) over(order by price_date rows between 29 preceding and current row))
            )
            / nullif(
                stddev_pop(gold_return::float8) over(order by price_date rows between 29 preceding and current row)
                * stddev_pop(btc_return::float8) over(order by price_date rows between 29 preceding and current row),
                0
            )
        as decimal(18, 6)), 2) as corr_btc_gold_30d

    from returns_pivoted
)

select *
from correlation
where day_number >= 30