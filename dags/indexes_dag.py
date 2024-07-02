from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import psycopg2

# Функции для расчета индексов
def rsi(prices, period):
    gains, losses = [0] * len(prices), [0] * len(prices)
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains[i] = change
        else:
            losses[i] = -change
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    rs_values = [None] * (period + 1)

    if avg_loss == 0:
        rs_values.append(100)
    else:
        rs = avg_gain / avg_loss
        rs_values.append(100 - (100 / (1 + rs)))

    for i in range(period + 1, len(prices)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rs_values.append(100)
        else:
            rs = avg_gain / avg_loss
            rs_values.append(100 - (100 / (1 + rs)))

    for i in range(len(rs_values)):
        if rs_values[i] is None:
            rs_values[i] = 0
    return rs_values

def ma(prices, window):
    values = []
    for i in range(len(prices)):
        if i + 1 < window:
            values.append(None)
        else:
            values.append(sum(prices[i + 1 - window:i + 1]) / window)
    return values

def exponential_ma(prices, window):
    ema = []
    k = 2 / (window + 1)
    ema.append(sum(prices[:window]) / window)
    for i in prices[window:]:
        ema.append((i - ema[-1]) * k + ema[-1])
    return ema

def macd(prices):
    ema_12 = exponential_ma(prices, 12)
    ema_26 = exponential_ma(prices, 26)
    macd_line = [ema12 - ema26 for ema12, ema26 in zip(ema_12[26-12:], ema_26)]
    signal_line = exponential_ma(macd_line, 9)
    return macd_line, [None] * 25 + signal_line

# Функция для проверки и вставки новых данных
def insert_indices(cursor, table_name, dates, rsis, mas, macds):
    for i in range(len(dates)):
        cursor.execute(f'SELECT COUNT(*) FROM {table_name} WHERE "Date_point" = %s', (dates[i],))
        if cursor.fetchone()[0] == 0:
            cursor.execute(f'INSERT INTO {table_name} ("Date_point", "rsi", "ma", "macd") VALUES (%s, %s, %s, %s)',
                           (dates[i], rsis[i], mas[i], macds[i]))

# Основная задача для расчета и вставки индексов
@task()
def calculate_and_insert_indices():
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    companies = {
        'IBM': 'International Business Machines Corp',
        'YNDX': 'Yandex',
        'MIME': 'Mimecast Ltd'
    }

    for symbol, company_name in companies.items():
        query = f'SELECT "Date_point", "Close" FROM companies_data WHERE "Name" = %s ORDER BY "Date_point"'
        cursor.execute(query, (company_name,))
        data = cursor.fetchall()
        dates = [row[0] for row in data]
        prices = [row[1] for row in data]

        if not prices:
            continue

        rsis = rsi(prices, 14)
        mas = ma(prices, 30)
        macd_line, _ = macd(prices)

        table_name = f'indexes_{symbol.lower()}'
        insert_indices(cursor, table_name, dates, rsis, mas, macd_line)

    connection.commit()
    cursor.close()
    connection.close()

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG('index_calculation_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    wait_for_primary = ExternalTaskSensor(
        task_id='wait_for_primary',
        external_dag_id='index_calculation_dag',  # ID основного DAG
        external_task_id='calculate_and_insert_indices',  # ID задачи в основном DAG
        timeout=600  # Ожидание до 10 минут
    )

    calculate_indices = calculate_and_insert_indices()
    wait_for_primary >> calculate_indices
