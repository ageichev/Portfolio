import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_domen_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["zones"] = top_data_df["domain"].str.split(".").str[-1]
    temp = top_data_df.groupby("zones", as_index=False).agg(cnt=("domain", "nunique")).sort_values("cnt", ascending=False)
    top_10_domen_zones = temp["zones"].head(10)
    with open('top_10_domen_zones.csv', 'w') as f:
        f.write(top_10_domen_zones.to_csv(index=False, header=False))


def get_longest_domen():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    lengths = top_data_df["domain"].str.len()
    argmax = np.where(lengths == lengths.max())[0]
    longest_domen = top_data_df.iloc[argmax]
    with open('longest_domen.csv', 'w') as f:
        f.write(longest_domen.to_csv(index=False, header=False))

def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        airflow_rank_num = top_data_df.loc[top_data_df['domain'] == "airflow.com"].index.item()
    except:
        airflow_rank_num = "Cannont find airflow.com"
    return airflow_rank_num


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domen_zones.csv', 'r') as f:
        all_domen_zones = f.read()
    with open('longest_domen.csv', 'r') as f:
        longest_dom = f.read()
    date = ds

    print(f'Top domains zones for date {date}')
    print(all_domen_zones)

    print(f'Longest domen for date {date}')
    print(longest_dom)

    print(get_airflow())

default_args = {
    'owner': 'i.shmelev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
    'schedule_interval': '* * */1 * *'
}
dag = DAG('i_shmelev_dag_lesson2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domen_zones',
                    python_callable=get_domen_zones,
                    dag=dag)

t2_lon = PythonOperator(task_id='get_longest_domen',
                        python_callable=get_longest_domen,
                        dag=dag)
t2_air = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_lon, t2_air] >> t3