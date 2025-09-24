import os, json, time, logging, urllib.parse
from typing import List, Tuple
import pendulum, requests

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator

TZ = pendulum.timezone("America/Argentina/Buenos_Aires")

# Fallback robusto: si env está vacía -> Variable -> default
COINS = (os.getenv("COINGECKO_COINS") or Variable.get("COINGECKO_COINS", default_var="ethereum,bitcoin")).replace(" ", "")
VS    = (os.getenv("COINGECKO_VS")    or Variable.get("COINGECKO_VS",    default_var="usd")).strip().lower()
API_KEY = (os.getenv("COINGECKO_API_KEY") or Variable.get("COINGECKO_API_KEY", default_var="")).strip()

HISTORY_URL = "https://api.coingecko.com/api/v3/coins/{coin}/history"

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="crypto_prices_coingecko_daily",
    description="Carga diaria de precios (history) desde CoinGecko a Postgres (tabla public.market_prices)",
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz=TZ),
    schedule="0 8 * * *",   # 08:00 AR
    catchup=False,
    default_args=default_args,
    tags=["crypto","coingecko","history"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_crypto",
        sql="""
        CREATE TABLE IF NOT EXISTS public.market_prices (
          ts   TIMESTAMP NOT NULL,   -- medianoche AR
          coin TEXT NOT NULL,
          vs   TEXT NOT NULL,
          price NUMERIC(18,8) NOT NULL,
          PRIMARY KEY (ts, coin, vs)
        );
        """,
        autocommit=True,
    )

    def fetch_and_upsert():
        # Normalizar parámetros
        coins_raw = (COINS or "").strip(", ")
        coins = [c.strip().lower() for c in coins_raw.split(",") if c.strip()]
        if not coins:
            raise ValueError(f"COINGECKO_COINS vacío/ inválido: '{COINS}'")
        vs = (VS or "").strip().lower()
        if not vs:
            raise ValueError(f"COINGECKO_VS vacío/ inválido: '{VS}'")

        ctx = get_current_context()
        logical_dt = ctx["logical_date"].in_timezone(TZ)
        ts_midnight = logical_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        date_str = ts_midnight.strftime("%d-%m-%Y")  # dd-mm-YYYY

        headers = {"Accept": "application/json", "User-Agent": "airflow-coingecko/1.0"}
        if API_KEY:
            headers["x-cg-pro-api-key"]  = API_KEY
            headers["x-cg-demo-api-key"] = API_KEY

        rows: List[Tuple] = []
        for coin in coins:
            url = HISTORY_URL.format(coin=coin)
            params = {"date": date_str, "localization": "false"}
            debug_url = url + "?" + urllib.parse.urlencode(params)
            logging.info("CoinGecko GET %s", debug_url)

            last = None
            for i in range(1, 4):
                r = requests.get(url, params=params, headers=headers, timeout=30)
                if r.status_code < 400:
                    break
                last = r
                logging.warning("HTTP %s for %s | body=%s", r.status_code, r.url, r.text[:400])
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2.0 * i)
                else:
                    break
            else:
                r = last

            if r is None or r.status_code >= 400:
                raise RuntimeError(f"CoinGecko error {getattr(r,'status_code','NA')} for {debug_url} -> {getattr(r,'text','no body')[:400]}")

            data = r.json()
            price = None
            try:
                price = data["market_data"]["current_price"].get(vs)
            except Exception:
                price = None
            if price is None:
                raise RuntimeError(f"Sin precio para coin={coin} vs={vs} en {date_str}. Resp={json.dumps(data)[:400]}")

            rows.append((ts_midnight.to_datetime_string(), coin.upper(), vs.upper(), float(price)))

        sql = """
        INSERT INTO public.market_prices (ts, coin, vs, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ts, coin, vs) DO UPDATE SET price = EXCLUDED.price;
        """
        PostgresHook(postgres_conn_id="postgres_crypto").run(sql, parameters=rows)

    fetch_task = PythonOperator(
        task_id="fetch_and_upsert",   # mantenemos el MISMO task_id que ya ve Airflow
        python_callable=fetch_and_upsert,
    )

    create_table >> fetch_task