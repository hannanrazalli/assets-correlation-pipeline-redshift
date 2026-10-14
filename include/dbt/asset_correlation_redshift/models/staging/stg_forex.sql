WITH raw_forex AS (
    SELECT * FROM {{ source('assets_sources', 'raw_forex') }}
),

renamed_and_casted AS (
    SELECT
        cast(date as date) as forex_date,
        cast(base as varchar(5)) as base_currency,
        cast(quote as varchar(5)) as target_currency,
        cast(rate as decimal(18,6)) as exchange_rate,

        {{ audit_columns('staging') }}
    FROM raw_forex
)

SELECT *
FROM renamed_and_casted