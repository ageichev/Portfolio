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

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat_reg():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["region"] = top_data_df["domain"].apply(lambda x: x[x.rfind(".") :])
    top_data_top_10_reg = top_data_df.groupby("region", as_index=False).agg({"domain" : "count"}).rename(columns={"domain": "quantity"}).sort_values("quantity", ascending=False).head(10)   
    with open('top_data_top_10_reg.csv', 'w') as f:
        f.write(top_data_top_10_reg.to_csv(index=False, header=False))


def get_top_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["len"] = top_data_df.domain.apply(lambda x: len(x))
    top_len = top_data_df.sort_values(["len", "domain"], ascending=[False, True]).head(1).iloc[0]["domain"]
    with open('top_len.txt', 'w') as f:
        f.write(top_len)

        
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df.query("domain == 'airflow.com'").shape[0] != 0:
        air = top_data_df.query("domain == 'airflow.com'")["rank"].values[0]
    else:
        air = "Domain is not existing in this file"
    with open('air.txt', 'w') as f:
        f.write(str(air))


def print_data(ds):
    with open('top_data_top_10_reg.csv', 'r') as f:
        top_reg = f.read()
    with open('top_len.txt', 'r') as f:
        top_len = f.read()
    with open('air.txt', 'r') as f:
        air = f.read()   
    date = ds

    print(f'Top domains by region for date {date}')
    print(top_reg)

    print(f'The longest domain for date {date}')
    print(top_len)
    
    print(f'Airflow.com rank for date {date}')
    print(air)

default_args = {
    'owner': 's-morozov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
}
schedule_interval = '15 18 * * *'

dag_morozov = DAG('top_10_test', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_morozov)

t2_a = PythonOperator(task_id='get_stat_reg',
                    python_callable=get_stat_reg,
                    dag=dag_morozov)

t2_b = PythonOperator(task_id='top_len',
                        python_callable=get_top_len,
                        dag=dag_morozov)

t2_c = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag_morozov)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_morozov) 

t1 >> [t2_a, t2_b, t2_c] >> t3
