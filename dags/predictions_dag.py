from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from tensorflow.keras.models import load_model
import psycopg2
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG('prediction_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    wait_for_index_calculation = ExternalTaskSensor(
        task_id='wait_for_primary',
        external_dag_id='prediction_dag',
        external_task_id='predict_and_store',  # ID задачи в DAG index_calculation_dag
        mode='poke',
        poke_interval=60,
        timeout=600
    )

    @task()
    def predict_and_store():
        models = {
            'IBM': 'LSTM_IBM_NICE (2).h5',
            'YNDX': 'LSTM_YANDEX_NICE.h5',
            'MIME': 'LSTM_MIME_NICE.h5'
        }

        company_names = {
            'IBM': 'International Business Machines Corp',
            'YNDX': 'Yandex',
            'MIME': 'Mimecast Ltd'
        }

        watch_back = 40

        pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        for symbol, model_file in models.items():
            model = load_model(model_file)
            company_name = company_names[symbol]
            query = f"SELECT * FROM companies_data WHERE \"Name\" = '{company_name}' ORDER BY \"Date_point\" DESC LIMIT {watch_back};"
            cursor.execute(query)
            results = cursor.fetchall()

            if len(results) < watch_back:
                continue

            columns = ['Date_point', 'Name', 'Open',
                       'High', 'Low', 'Close', 'Volume']
            df = pd.DataFrame(results, columns=columns)
            df['Date_point'] = pd.to_datetime(df['Date_point'])
            df = df.sort_values(by='Date_point')

            predict_data = df[['Open', 'High', 'Low', 'Close', 'Volume']]
            np_predict_data = np.array(predict_data)
            np_predict_data = np_predict_data.reshape((1, watch_back, 5))
            normalizer = MinMaxScaler(feature_range=(0, 1))
            np_predict_data_tmp = np_predict_data.reshape(
                -1, np_predict_data.shape[-1])
            scaled_predict = normalizer.fit_transform(np_predict_data_tmp)
            x_train = scaled_predict.reshape(np_predict_data.shape)

            results = model.predict(x_train)
            dates_df = df['Date_point']
            start_date = dates_df.iloc[-1]
            dates = pd.date_range(start=start_date, periods=91, freq='D')
            dates = dates.strftime('%Y-%m-%d').tolist()[1:]

            query_check = 'SELECT COUNT(*) FROM predictions WHERE "company_name" = %s AND "date_of_predict" = %s'
            query_insert = 'INSERT INTO predictions ("company_name", "date_of_predict", "close") VALUES (%s, %s, %s)'

            for i in range(len(results[0])):
                name = company_name
                predict_date = datetime.strptime(dates[i], '%Y-%m-%d')
                close = float(results[0][i])
                cursor.execute(query_check, (name, predict_date))
                if cursor.fetchone()[0] == 0:
                    cursor.execute(query_insert, (name, predict_date, close))

        connection.commit()
        cursor.close()
        connection.close()

    wait_for_index_calculation >> predict_and_store()
