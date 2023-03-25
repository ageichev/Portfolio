import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_temp = top_data_df.domain.str.split('.', expand = True)
    df_temp.columns = ['name', 'domain', 'third', 'forth', 'fifth']

def question1():
    answear1 = df_temp.groupby('domain', as_index = False).agg({'name':'count'}) \
        .sort_values(by = 'name', ascending = False).head(10).reset_index(drop = True)
    print(answear1)

def question2():
    answear2 = df_temp.loc[df_temp['name'].str.len() == df_temp['name'].str.len().max()].name.values
    print(answear2)

def question3():
    answear3 = top_data_df.query('domain == "airflow.com"')
    if len(answear3) == 0:
        answear3 = 'airflow.com is not found'
    print(answear3)

default_args = {
    'owner': 'v-kachan-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 29),
}

schedule_interval = '0 0 * * *'

dag = DAG('v_kachan_26_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='q1',
                    python_callable=question1,
                    dag=dag)

t3 = PythonOperator(task_id='q2',
                    python_callable=question2,
                    dag=dag)

t4 = PythonOperator(task_id='q3',
                    python_callable=question3,
                    dag=dag)

t1 >> [t2, t3, t4]

