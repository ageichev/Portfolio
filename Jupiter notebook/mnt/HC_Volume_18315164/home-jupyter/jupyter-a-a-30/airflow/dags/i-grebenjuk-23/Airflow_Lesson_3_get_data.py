import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

data_link = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
data_file = 'vgsales.csv'


def get_and_add_data():
    df = pd.read_csv(data_link) # Считали данные
    df = df.query('Year==2006')
    df_csv = df.to_csv(index=False)
    with open(data_file, 'w') as f:
        f.write(df_csv)

def print_data(ds):
    with open('vgsales.csv', 'r') as f:
        all_data_2006 = f.read()

    date = ds

    print(f'The number of rows for date {date} is equal to:')
    print(all_data_2006)

default_args = {
    'owner': 'i-grebenjuk-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 31),
}

schedule_interval = '00 08 * * *'

dag = DAG('Lesson_3_getdatacheck_igorG', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_and_add_data',
                    python_callable=get_and_add_data,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t3

