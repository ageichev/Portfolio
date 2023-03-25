import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# аа
def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['dom_zone'] = top_data_df.domain.str.split('.').str[-1]
    top_data_top_10 = top_data_df.groupby('dom_zone').domain.nunique().sort_values(ascending=False)
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=True, header=False))
        
def get_top_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['dom_len'] = top_data_df.domain.str.len().sort_values(ascending=False)
    top_dom_len = top_data_df.sort_values(['dom_len','domain'],ascending=[False,True])
    top_dom_len = top_dom_len.domain.head(1)
    with open('top_dom_len.csv', 'w') as f:
        f.write(top_dom_len.to_csv(index=False, header=False))
        
def get_airflow_position():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['dom_len'] = top_data_df.domain.str.len().sort_values(ascending=False)
    top_dom_len = top_data_df.sort_values(['dom_len','domain'],ascending=[False,True])
    top_dom_len['position'] = np.arange(len(top_dom_len))
    airflow_pos1 = top_dom_len[top_dom_len.domain == 'airflow.com']
    airflow_pos1 = airflow_pos1.apply(lambda x: 'not found airflow in the list' if len(x.value_counts()) == 0  else x)
#     airflow_pos1 = pd.DataFrame(airflow_pos1).reset_index()
#     airflow_pos1 = airflow_pos.position.apply(lambda x: 'not found airflow in the list' if ff.empty  else x)
    with open('airflow_pos1.csv', 'w') as f:
        f.write(airflow_pos1.to_csv(index=False, header=False))
        
# def get_stat():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


# def get_stat_com():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10_com.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_dom_len.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow_pos1.csv', 'r') as f:
        all_data_pos = f.read()
    date = ds

    print(f'Top 10 doamin zones by quantity of domens for date {date}')
    print(all_data)

    print(f'The longest domain name for date {date}')
    print(all_data_len)
    
    print(f'The place of airflow.com for date {date}')
    print(all_data_pos)


default_args = {
    'owner': 'v.syropyatov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 26),
}
schedule_interval = '0 17 * * *'

dag = DAG('v_syropyatov_air1', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_dom',
                    python_callable=get_top_10_dom,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_len',
                    python_callable=get_top_len,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_position',
                        python_callable=get_airflow_position,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)