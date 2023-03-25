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

        

def top_10_domain_zones_dgati():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').apply(lambda x: x[-1])
    top_10_dz =  top_data_df.groupby('domain_zone', as_index=False) \
                        .agg({'domain': 'count'}) \
                        .rename(columns={'domain': 'count_of_names'}) \
                        .sort_values('count_of_names', ascending=False) \
                        .head(10)[['domain_zone']]
    with open('top_10_domain_zones_dgati.csv', 'w') as f:
        f.write(top_10_dz.to_csv(index=False, header=False))
        
def get_longest_name_dgati():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(lambda x: len(x))
    longest_name =  top_data_df[top_data_df.domain_length == top_data_df.domain_length.max()] \
                                                                .sort_values('domain') \
                                                                .head(1) \
                                                                .domain
    with open('longest_name_dgati.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))

def where_is_airflow_dgati():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    temp_df = top_data_df[top_data_df['domain'].apply(lambda x: True if 'airflow.com' in x else False)]
    if len(temp_df) > 0:
        where_is_airflow = top_data_df[top_data_df['domain'].apply(lambda x: True if 'airflow.com' in x else False)] \
                                .reset_index()['rank']
    else:
        where_is_airflow = pd.Series('There is no such domain!')
        
    with open('where_is_airflow.csv', 'w') as f:
        f.write(where_is_airflow.to_csv(index=False, header=False))
    



def print_data(ds):
#     with open('top_data_top_10.csv', 'r') as f:
#         all_data = f.read()
#     with open('top_data_top_10_com.csv', 'r') as f:
#         all_data_com = f.read()
    
    with open('top_10_domain_zones_dgati.csv', 'r') as f:
        top_10_domain_zones_dgati = f.read()
    with open('longest_name_dgati.csv', 'r') as f:
        longest_name_dgati = f.read()
    with open('where_is_airflow.csv', 'r') as f:
        where_is_airflow = f.read() 
    
    date = ds

    print(f'Top 10 domains zones for date {date} are:')
    print(top_10_domain_zones_dgati)

    print(f'Domain with longest name for date {date} is:')
    print(longest_name_dgati)
    
    print(f'Rank of airflow.com for date {date} is:')
    print(where_is_airflow)


default_args = {
    'owner': 'd.gatiatullin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 20),
}
schedule_interval = '0 17 * * *'

dag = DAG('dgati_dag1', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zones_dgati',
                    python_callable=top_10_domain_zones_dgati,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name_dgati',
                        python_callable=get_longest_name_dgati,
                        dag=dag)

t4 = PythonOperator(task_id='where_is_airflow_dgati',
                        python_callable=where_is_airflow_dgati,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5