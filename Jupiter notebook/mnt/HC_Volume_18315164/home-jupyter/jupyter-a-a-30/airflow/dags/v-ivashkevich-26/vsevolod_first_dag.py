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


def get_top_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_zones_df = top_data_df.domain.str.split(pat = '.', expand=True)
    top_domain_zones_df = top_domain_zones_df[[0, 1]]
    top_domain_zones_df = top_domain_zones_df.groupby( 1, as_index = False)\
                                             .agg({0 : 'count'})\
                                             .sort_values(0, ascending= False)\
                                             .head(10)
    with open('top_domain_zones.csv', 'w') as f:
        f.write(top_domain_zones_df.to_csv(index=False, header=False))


def get_long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len = top_data_df.domain.str.len().max()
    top_data_df['len'] = top_data_df.domain.str.len()
    long_name = top_data_df[top_data_df.len == max_len].sort_values('domain').head(1)
    long_name = long_name[['domain']]
    with open('long_name.csv', 'w') as f:
        f.write(long_name.to_csv(index=False, header=False))
        

def airflow_rank_in_list():      
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    airflow_rank_in_list = top_data_df[top_data_df['domain'].str.startswith('airflow.com')].head(1)
    with open('airflow_rank_in_list.csv', 'w') as f:
        f.write(airflow_rank_in_list.to_csv(index=False))


def print_data(ds):
   
    with open('long_name.csv', 'r') as f:
        long_name = f.read()
    with open('airflow_rank_in_list.csv', 'r') as f:
        all_data_com = f.read()
    with open('top_domain_zones.csv', 'r') as f:
        top_domain_zones = f.read()
    
    date = ds

    print(f'Top domains for date {date}')
    print(top_domain_zones)

    print(f'Top long name for date {date}')
    print(long_name)
    

    try:
        print(airflow_rank_in_list['rank'].to_list()[0] , 'is position Airflow in list domains')
    except:
        print("Airflow.com is missing in list domains")


default_args = {
    'owner': 'v.ivashkevich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16),
}
schedule_interval = '0 12 * * *'

dag = DAG('v-ivashkevich_2_lesson', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_zones = PythonOperator(task_id='get_top_domain_zones',
                    python_callable=get_top_domain_zones,
                    dag=dag)

t2_long_name = PythonOperator(task_id='get_long_name',
                        python_callable=get_long_name,
                        dag=dag)

t2_airflow_rank = PythonOperator(task_id='airflow_rank_in_list',
                        python_callable=airflow_rank_in_list,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_zones, t2_long_name, t2_airflow_rank ] >> t3
