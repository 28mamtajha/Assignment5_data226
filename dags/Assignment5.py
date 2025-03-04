# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG # type: ignore
from airflow.models import Variable # type: ignore
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():
    """Returns a Snowflake cursor using Airflow's SnowflakeHook"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract_stock_prices(symbol):
    """Fetch last 90 days of stock prices from Alpha Vantage API"""
    api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    if "Time Series (Daily)" not in data:
        raise ValueError("Invalid API response, check your API key and symbol.")

    records = []
    for date in sorted(data["Time Series (Daily)"].keys(), reverse=True)[:90]:
        daily_data = data["Time Series (Daily)"][date]
        records.append([
            symbol, date, 
            float(daily_data['1. open']), 
            float(daily_data['2. high']), 
            float(daily_data['3. low']), 
            float(daily_data['4. close']), 
            int(daily_data['5. volume'])
        ])
    
    return records


@task
def load_to_snowflake(records, target_table):
    """Load stock price data into Snowflake using SQL transaction (full refresh)"""
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INT,
            PRIMARY KEY (symbol, date)
        );
        """)
        cur.execute(f"""
        DELETE FROM {target_table} 
        WHERE date >= (CURRENT_DATE - INTERVAL '90 days');
        """)

        for r in records:
            sql = f"""
            INSERT INTO {target_table} (symbol, date, open, high, low, close, volume) 
            VALUES ('{r[0]}', '{r[1]}', {r[2]}, {r[3]}, {r[4]}, {r[5]}, {r[6]})
            """
            cur.execute(sql)

        cur.execute("COMMIT;")
        print("Data successfully inserted into Snowflake.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Error loading data:", e)
        raise e


with DAG(
    dag_id="StockPrice_ETL",
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ETL', 'StockPrice'],
    schedule="30 2 * * *"
) as dag:
    
    target_table = "dev.raw_data.stock_price"
    stock_symbol = "NVDA"

    stock_data = extract_stock_prices(stock_symbol)
    load_to_snowflake(stock_data, target_table)
