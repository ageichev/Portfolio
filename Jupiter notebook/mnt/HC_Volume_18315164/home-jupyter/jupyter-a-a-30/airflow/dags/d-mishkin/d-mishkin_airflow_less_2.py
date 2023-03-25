import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    #      ,         
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_dom_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.extract('\.(\w+)')
    dom_zone_top_10 = top_data_df.groupby('zone', as_index=False).count()[['zone', 'domain']].sort_values('domain', ascending=False)
    dom_zone_top_10 = dom_zone_top_10.head(10)
    with open('dom_zone_top_10.csv', 'w') as f:
        f.write(dom_zone_top_10.to_csv(index=False, header=False))

def get_top_1_len_dom_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain'] = top_data_df['domain'].astype('str')
    top_data_df['len'] = top_data_df.domain.str.len()
    top_1_len_dom_name = top_data_df.sort_values(['len', 'domain'], ascending=False).head(1)
    with open('top_1_len_dom_name.csv', 'w') as f:
        f.write(top_1_len_dom_name.to_csv(index=False, header=False))        
        
        
def get_airflow_position():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain'] = top_data_df['domain'].astype('str')
    if top_data_df.query('domain == "airflow.com"').shape[0] == 0:
        with open('file_get_airflow_position.csv', 'w') as f:
            f.write('airflow.com is not found')
    else:
        with open('file_get_airflow_position.csv', 'w') as f:
            f.write(file_get_airflow_position.to_csv(index=False, header=False))
        
def print_data(ds):
    with open('dom_zone_top_10.csv', 'r') as f:
        all_data_zone = f.read()
    with open('top_1_len_dom_name.csv', 'r') as f:
        all_data_len = f.read()
    with open('file_get_airflow_position.csv', 'r') as f:
        all_data_position = f.read()
    date = ds

    print(f'Top domains zone for date {date}')
    print(all_data_zone)

    print(f'The longest domain name for date {date}')
    print(all_data_len)

    print(f'Airflow position for date {date}')
    print(all_data_position)    
    
default_args = {
    'owner': 'd.mishkin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 26),
}
schedule_interval = '0 17 * * *'

dag = DAG('d-mishkin_top_10_ru', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_dom_zone',
                    python_callable=get_top_10_dom_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_1_len_dom_name',
                    python_callable=get_top_1_len_dom_name,
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
