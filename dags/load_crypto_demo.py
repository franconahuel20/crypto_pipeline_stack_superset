from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 0}

with DAG(
    dag_id="load_crypto_demo",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["demo", "crypto"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_crypto",
        sql="""
        CREATE TABLE IF NOT EXISTS public.market_prices (
          ts TIMESTAMP NOT NULL,
          coin TEXT NOT NULL,
          price_usd NUMERIC(18,6) NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_market_prices ON public.market_prices(ts, coin);
        """
    )

    seed_data = PostgresOperator(
        task_id="seed_data",
        postgres_conn_id="postgres_crypto",
        sql="""
        INSERT INTO public.market_prices (ts, coin, price_usd) VALUES
          (NOW() - INTERVAL '6 days','BTC',65432.12),
          (NOW() - INTERVAL '5 days','BTC',66210.55),
          (NOW() - INTERVAL '4 days','BTC',65100.30),
          (NOW() - INTERVAL '3 days','BTC',64220.90),
          (NOW() - INTERVAL '2 days','BTC',64888.75),
          (NOW() - INTERVAL '1 day','BTC',65510.40),
          (NOW() - INTERVAL '6 days','ETH',3401.22),
          (NOW() - INTERVAL '5 days','ETH',3388.10),
          (NOW() - INTERVAL '4 days','ETH',3420.05),
          (NOW() - INTERVAL '3 days','ETH',3359.80),
          (NOW() - INTERVAL '2 days','ETH',3399.15),
          (NOW() - INTERVAL '1 day','ETH',3435.40)
        ON CONFLICT (ts, coin) DO UPDATE SET price_usd = EXCLUDED.price_usd;
        """
    )

    create_table >> seed_data