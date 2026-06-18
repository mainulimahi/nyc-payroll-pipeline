import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

from src.extract import main as fetch_api_data
from src.transform import get_spark_session, process_data


CREATE_PUBLIC_TABLE = """
    CREATE TABLE IF NOT EXISTS public.nyc_payroll (
        fiscal_year                  INT,
        payroll_number               INT,
        agency_name                  VARCHAR(200),
        last_name                    VARCHAR(100),
        first_name                   VARCHAR(100),
        mid_init                     VARCHAR(10),
        agency_start_date            DATE,
        work_location_borough        VARCHAR(100),
        title_description            VARCHAR(200),
        leave_status_as_of_june_30   VARCHAR(50),
        base_salary                  FLOAT,
        pay_basis                    VARCHAR(50),
        regular_hours                FLOAT,
        regular_gross_paid           FLOAT,
        ot_hours                     FLOAT,
        total_ot_paid                FLOAT,
        total_other_pay              FLOAT,
        loaded_at                    TIMESTAMP
    );
"""


def api_to_db():
    """Fetch from NYC API → insert into staging.nyc_payroll"""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    df, create_table_sql = fetch_api_data()

    if df.empty:
        raise AirflowFailException("API returned an empty dataset — aborting.")

    conn = hook.get_conn()
    cur = conn.cursor()

    # Step 1: Create schema separately and commit first
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()

    # Step 2: Create table + truncate in second transaction
    cur.execute(create_table_sql)
    cur.execute("TRUNCATE TABLE staging.nyc_payroll;")
    conn.commit()
    cur.close()

    rows = list(df.itertuples(index=False, name=None))
    hook.insert_rows(table="staging.nyc_payroll", rows=rows)
    print(f"Inserted {len(rows)} rows into staging.nyc_payroll")


def validate_staging():
    """Sanity checks on staging data before processing."""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    df = hook.get_pandas_df(sql="SELECT * FROM staging.nyc_payroll")

    if len(df) == 0:
        raise AirflowFailException("staging.nyc_payroll is empty.")

    bad_years = df[(df["fiscal_year"] < 2000) | (df["fiscal_year"] > 2100)]
    if not bad_years.empty:
        raise AirflowFailException(
            f"Found {len(bad_years)} rows with invalid fiscal_year values."
        )

    negative_values = df[(df["regular_hours"] < 0) | (df["regular_gross_paid"] < 0)]
    if not negative_values.empty:
        raise AirflowFailException(
            f"Found {len(negative_values)} rows with negative hours/pay values."
        )

    print(f"Validation passed: {len(df)} rows look good.")


def db_to_db():
    """Read staging → Spark processing → append into public.nyc_payroll"""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    now = datetime.utcnow()

    df = hook.get_pandas_df(sql="SELECT * FROM staging.nyc_payroll")
    print(f"Read {len(df)} rows from staging")

    spark = get_spark_session()
    processed_df = process_data(spark=spark, df=df)

    if processed_df.empty:
        raise AirflowFailException("Spark processing returned an empty dataset.")

    # Tag each row with current timestamp
    processed_df["loaded_at"] = now

    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(CREATE_PUBLIC_TABLE)
    conn.commit()
    cur.close()

    rows = list(processed_df.itertuples(index=False, name=None))
    hook.insert_rows(table="public.nyc_payroll", rows=rows)
    print(f"Inserted {len(rows)} rows at {now} into public.nyc_payroll")


def validate_final():
    """Sanity check on the final table after each run."""
    hook = PostgresHook(postgres_conn_id="postgres_conn")

    # Check table exists first
    table_exists = hook.get_first(
        sql="""SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'nyc_payroll'
        )"""
    )[0]

    if not table_exists:
        raise AirflowFailException("public.nyc_payroll table does not exist.")

    # Get the latest batch timestamp
    latest_ts = hook.get_first(
        sql="SELECT MAX(loaded_at) FROM public.nyc_payroll"
    )[0]

    if not latest_ts:
        raise AirflowFailException("No rows found in public.nyc_payroll.")

    # Count rows in the latest batch
    latest_count = hook.get_first(
        sql=f"SELECT COUNT(*) FROM public.nyc_payroll WHERE loaded_at = '{latest_ts}'"
    )[0]

    staging_count = hook.get_first(
        sql="SELECT COUNT(*) FROM staging.nyc_payroll"
    )[0]

    if latest_count < (staging_count * 0.3):
        raise AirflowFailException(
            f"Latest batch ({latest_count}) is suspiciously low "
            f"compared to staging ({staging_count})."
        )

    total_count = hook.get_first(
        sql="SELECT COUNT(*) FROM public.nyc_payroll"
    )[0]

    print(f"Latest batch: {latest_count} rows at {latest_ts}. Total historical: {total_count} rows.")


with DAG(
    dag_id="nyc_payroll_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc", "payroll", "spark"],
    default_args={
        "email": [os.environ.get("ALERT_EMAIL")],
        "email_on_failure": True,
        "email_on_retry": False,
    },
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    api_to_db_task = PythonOperator(
        task_id="api_to_db",
        python_callable=api_to_db,
    )

    validate_staging_task = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )

    db_to_db_task = PythonOperator(
        task_id="db_to_db",
        python_callable=db_to_db,
    )

    validate_final_task = PythonOperator(
        task_id="validate_final",
        python_callable=validate_final,
    )

    start >> api_to_db_task >> validate_staging_task >> db_to_db_task >> validate_final_task >> end