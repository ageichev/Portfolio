import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_zones(ds):
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.', 1)[1].lower())
    top_data_df['domain_name'] = top_data_df['domain'].apply(lambda x: x.split('.', 1)[0].lower())
    top_data_group = top_data_df.groupby('zone', as_index=False).agg({'domain_name': 'count'}).sort_values('domain_name', ascending=False).head(10)
    sr = top_data_group.to_string(columns=['zone'], header=False, index=False)
    with open('params_top10.txt', 'w') as f:
        f.write(f'Top 10 domain zones for date {ds} are:'  + '\n')  
        f.write(sr + '\n')  
    
def get_the_longest(ds):
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    mxl = top_data_df['domain'].str.len().max()
    the_longest = top_data_df[top_data_df['domain'].str.len().between(mxl, mxl)].iloc[0]['domain']
    search_name = 'airflow.com'
    answ_str = f'The place in rating by length of "AirFlow.com" for date {ds} is '
    if (top_data_df['domain']==search_name).any():
        top_data_df['dom_len'] = top_data_df['domain'].str.len()
        top_data_df = top_data_df.drop_duplicates().sort_values('dom_len', ascending=False).reset_index()
        place = top_data_df.loc[top_data_df['domain']==search_name].index[0]
        place_air = (f'{answ_str}{place}')
    else:
        place_air = (f'{answ_str}Not found')
    with open('params.txt', 'w') as f:
        f.write(f"The longest domain zone name for date {ds} is '{the_longest}'"  + '\n')  
        f.write(place_air) 
 
def print_data():  
    with open('params_top10.txt', 'r') as f:
        for line in f:
            print(line) 
    with open('params.txt', 'r') as f:
        for line in f:
            print(line) 

default_args = {
    'owner': 'n-zhitnikova-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 6),
}
schedule_interval = '0 8 * * *'

dag = DAG('n_zhitnikova_20_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t2_com = PythonOperator(task_id='get_the_longest',
                        python_callable=get_the_longest,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_com] >> t3
