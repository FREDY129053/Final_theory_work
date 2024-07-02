from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

api_key = '61O5P07NNP5BYSU2'
companies = {
    'IBM': 'International Business Machines Corp',
    'YNDX': 'Yandex',
    'MIME': 'Mimecast Ltd'
}

def get_last_date(symbol, cursor):
    cursor.execute("""
        SELECT MAX("Date_point") 
        FROM companies_data 
        WHERE "Name" = %s
    """, (companies[symbol],))
    last_date = cursor.fetchone()[0]
    if last_date is None:
        return None
    return last_date + timedelta(days=1)

@task()
def fetch_and_store_data():
    pg_hook = PostgresHook(postgres_conn_id='data_mart')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for symbol in companies.keys():
        last_date = get_last_date(symbol, cursor)
        
        if last_date:
            start_date = last_date.strftime('%Y-%m-%d')
        else:
            start_date = '2000-01-01'  # Начальная дата, если данных нет
        
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response = requests.get(url).json()
        data = response.get('Time Series (Daily)', {})

        for date_str, values in data.items():
            date_point = datetime.strptime(date_str, '%Y-%m-%d')
            if date_point >= last_date:
                query = """
                    INSERT INTO companies_data ("Date_point", "Name", "Open", "High", "Low", "Close", "Volume") 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    date_point, 
                    companies[symbol], 
                    float(values['1. open']), 
                    float(values['2. high']), 
                    float(values['3. low']), 
                    float(values['4. close']), 
                    float(values['5. volume'])
                ))

        connection.commit()

    cursor.close()
    connection.close()

with DAG('daily_stock_data_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    fetch_and_store_data()
