import os
import json
from typing import List, Tuple
import pendulum
import requests

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator

TZ = pendulum.timezone("America/Argentina/Buenos_Aires")
COINS = os.getenv("COINGECKO_COINS", "bitcoin,ethereum").replace(" ", "")
VS = os.getenv("COINGECKO_VS", "usd")
API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
API_URL = "https://api.coingecko.com/api/v3/simple/price"

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="crypto_prices_coingecko_daily",
    description="Carga diaria de precios desde CoinGecko a Postgres (tabla public.market_prices)",
    start_date=pendulum.datetime(2025, 1, 1, 0, 0, tz=TZ),
    schedule="0 8 * * *",   # 08:00 AR todos los días
    catchup=False,
    default_args=default_args,
    tags=["crypto","coingecko"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_crypto",
        sql="""
        CREATE TABLE IF NOT EXISTS public.market_prices (
          ts TIMESTAMP NOT NULL,
          coin TEXT NOT NULL,
          vs  TEXT NOT NULL,
          price NUMERIC(18,8) NOT NULL,
          PRIMARY KEY (ts, coin, vs)
        );
        """,
        autocommit=True,
    )

    def fetch_and_upsert():
        # Obtener contexto de ejecución (2.9+)
        context = get_current_context()
        logical_date = context["logical_date"].in_timezone(TZ)
        ts = logical_date.replace(minute=0, second=0, microsecond=0)

        params = {"ids": COINS, "vs_currencies": VS}
        headers = {}
        if API_KEY:
            headers["x-cg-pro-api-key"] = API_KEY
            headers["x-cg-demo-api-key"] = API_KEY

        resp = requests.get(API_URL, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        rows: List[Tuple] = []
        for coin_id, mapping in data.items():
            price = mapping.get(VS)
            if price is None:
                continue
            rows.append((ts.to_datetime_string(), coin_id.upper(), VS.upper(), float(price)))

        if not rows:
            raise ValueError(f"CoinGecko vacío para ids={COINS} vs={VS}: {json.dumps(data)}")

        insert_sql = """
        INSERT INTO public.market_prices (ts, coin, vs, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ts, coin, vs) DO UPDATE SET price = EXCLUDED.price;
        """
        hook = PostgresHook(postgres_conn_id="postgres_crypto")
        hook.run(insert_sql, parameters=rows)

    fetch_task = PythonOperator(
        task_id="fetch_and_upsert",
        python_callable=fetch_and_upsert,
    )

    create_table >> fetch_task
