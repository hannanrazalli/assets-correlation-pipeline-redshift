import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from cosmos import DbtTaskGroup
from pendulum import datetime

from include.utilities.cosmos_config import (
    execution_config,
    profile_config,
    project_config,
    render_config,
)
from include.utilities.slack_alerts import slack_failure_alert

BUCKET = os.getenv("BUCKET_NAME")

default_args = {
    "owner": "Hannan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack_failure_alert,
}


def has_files(name, logical_date, **_):
    prefix = (
        f"raw/{name}/"
        f"year={logical_date:%Y}/month={logical_date:%m}/day={logical_date:%d}/"
    )
    return bool(S3Hook().list_keys(bucket_name=BUCKET, prefix=prefix))


@dag(
    dag_id="00_Daily_Asset_Pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 10, 2),
    catchup=False,
    max_active_runs=1,
)
def asset_pipeline():
    # ---------- 1. Ingestion ----------
    @task(task_id="daily_forex")
    def daily_forex(ds=None):
        from include.ingestions.api.forex import forex

        forex(ds=ds)

    @task(task_id="daily_stocks")
    def daily_stocks(ds=None):
        from include.ingestions.api.stocks import stocks

        stocks(ds=ds)

    @task(task_id="daily_bitcoin")
    def daily_bitcoin(ds=None):
        from include.ingestions.api.bitcoin import bitcoin

        bitcoin(ds=ds)

    @task(task_id="daily_gold")
    def daily_gold(ds=None):
        from include.ingestions.api.gold import gold

        gold(ds=ds)

    # ---------- 2. Check S3 files ----------
    check_forex = ShortCircuitOperator(
        task_id="check_forex_has_data",
        python_callable=has_files,
        op_kwargs={"name": "forex"},
        ignore_downstream_trigger_rules=False,
    )
    check_stocks = ShortCircuitOperator(
        task_id="check_stocks_has_data",
        python_callable=has_files,
        op_kwargs={"name": "stocks"},
        ignore_downstream_trigger_rules=False,
    )
    check_bitcoin = ShortCircuitOperator(
        task_id="check_bitcoin_has_data",
        python_callable=has_files,
        op_kwargs={"name": "bitcoin"},
        ignore_downstream_trigger_rules=False,
    )
    check_gold = ShortCircuitOperator(
        task_id="check_gold_has_data",
        python_callable=has_files,
        op_kwargs={"name": "gold"},
        ignore_downstream_trigger_rules=False,
    )

    # ---------- 3. Load S3 -> Redshift ----------
    load_forex = S3ToRedshiftOperator(
        task_id="load_forex_to_redshift",
        schema="raw_data",
        table="raw_forex",
        s3_bucket=BUCKET,
        s3_key="raw/forex/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/day={{ logical_date.strftime('%d') }}/",
        copy_options=["FORMAT AS JSON 'auto'"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    load_stocks = S3ToRedshiftOperator(
        task_id="load_stocks_to_redshift",
        schema="raw_data",
        table="raw_stocks",
        s3_bucket=BUCKET,
        s3_key="raw/stocks/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/day={{ logical_date.strftime('%d') }}/",
        copy_options=["FORMAT AS PARQUET"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    load_bitcoin = S3ToRedshiftOperator(
        task_id="load_bitcoin_to_redshift",
        schema="raw_data",
        table="raw_bitcoin",
        s3_bucket=BUCKET,
        s3_key="raw/bitcoin/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/day={{ logical_date.strftime('%d') }}/",
        copy_options=["FORMAT AS PARQUET"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    load_gold = S3ToRedshiftOperator(
        task_id="load_gold_to_redshift",
        schema="raw_data",
        table="raw_gold",
        s3_bucket=BUCKET,
        s3_key="raw/gold/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/day={{ logical_date.strftime('%d') }}/",
        copy_options=["FORMAT AS PARQUET"],
        redshift_conn_id="redshift_default",
        method="APPEND",
    )

    # ---------- 4. dbt ----------
    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": True, "trigger_rule": "none_failed"},
    )

    # ---------- 5. Workflows ----------
    daily_forex() >> check_forex >> load_forex
    daily_stocks() >> check_stocks >> load_stocks
    daily_bitcoin() >> check_bitcoin >> load_bitcoin
    daily_gold() >> check_gold >> load_gold

    [load_forex, load_stocks, load_bitcoin, load_gold] >> dbt_build


asset_pipeline()
