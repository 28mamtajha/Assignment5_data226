from airflow import DAG # type: ignore
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator # type: ignore
from datetime import datetime
from airflow.utils.dates import days_ago # type: ignore

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from airflow.models import Variable # type: ignore
from airflow.decorators import task # type: ignore
import requests # type: ignore

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()


@task
def extract(symbols):
    all_results = {}
    for stock_symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={api_key}'
        res = requests.get(url).json()
        results = []
        for d in res["Time Series (Daily)"]:
            daily_info = res["Time Series (Daily)"][d]
            daily_info['6. date'] = d
            daily_info['7. stock_symbol'] = stock_symbol
            results.append(daily_info)
        all_results[stock_symbol] = results[-180:]
    return all_results


@task
def transform(data):
    entries = []
    for stock_symbol, entry_list in data.items():
        entries.extend(entry_list)
    return entries

@task
def load(con, results, target_table):
  try:
    con.execute("BEGIN")
    con.execute(f"""CREATE OR REPLACE TABLE {target_table} (
            stock_symbol VARCHAR(10) NOT NULL,
            date TIMESTAMP_NTZ NOT NULL,
            open DECIMAL(10, 4) NOT NULL,
            high DECIMAL(10, 4) NOT NULL,
            low DECIMAL(10, 4) NOT NULL,
            close DECIMAL(10, 4) NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (stock_symbol,date)
        )""")
    for r in results:
        open =   r['1. open'].replace("'", "''")
        high =   r['2. high'].replace("'", "''")
        low =    r['3. low'].replace("'", "''")
        close =  r['4. close'].replace("'", "''")
        volume = r['5. volume'].replace("'", "''")
        date =   r['6. date'].replace("'", "''")
        stock_symbol = r['7. stock_symbol'].replace("'", "''")

        sql = f"INSERT INTO {target_table} (stock_symbol, date, open, high, low, close, volume) VALUES ('{stock_symbol}', '{date}', '{open}', '{high}', '{low}', '{close}', '{volume}')"
        con.execute(sql)
    con.execute("COMMIT")
  except Exception as e:
        con.execute("ROLLBACK")
        print(e)
        raise(e)
  
@task
def train_model(cur, train_input_table, train_view, forecast_function_name):
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, STOCK_SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'STOCK_SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)

        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT STOCK_SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '"', '') as STOCK_SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


with DAG(
    dag_id = 'AlphaVantage_Stock_Price_Prediction_Analysis',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ELT','ML'],
    schedule = '15 00 * * *'
) as dag:
    target_table = "dev.raw_data.alphavantage_stockprice"
    api_key = Variable.get("alphavantage_apikey")
    my_conn = return_snowflake_conn()
    symbols = ['NVDA', 'MSFT']

    train_input_table = "dev.raw_data.alphavantage_stockprice"
    train_view = "dev.curation.training_view"
    forecast_table = "dev.curation.stockprice_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.stockprice_finalprediction"

    extract_task = extract(symbols)
    transformed_task = transform(extract_task)
    load_task = load(my_conn, transformed_task, target_table)
    train_task = train_model(my_conn, train_input_table, train_view, forecast_function_name)
    predict_task = predict(my_conn, forecast_function_name, train_input_table, forecast_table, final_table)

    extract_task >> transformed_task >> load_task >> train_task >> predict_task

