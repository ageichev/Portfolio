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



def topchik10():
    topchik = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    topchik['domain_zone'] = topchik['domain'].str.split('.').str[-1]
    topchikZone = topchik \
        .groupby('domain_zone', as_index=False) \
        .agg({'rank': 'count'}) \
        .sort_values('rank', ascending=False) \
        .head(10)
   
    with open('topchik10.csv', 'w') as f:
        f.write(topchikZone.to_csv(index=False, header=False))


def long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_data_df['domain_lenght'] = top_data_df['domain'].apply(len)
    top_data_df[['domain','domain_lenght']].sort_values('domain_lenght', ascending=False).reset_index().loc[0]['domain']
    
   
    with open('long_name.csv', 'w') as f:
        f.write(long_name.to_csv(index=False, header=False))

def airrank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank','domain'])
    
    with open('airflow_rank.csv', 'w') as f:
        if top_data_df[top_data_df['domain']=='airflow.com'].empty:
            f.write('airflow.com is not found')
        else:
            airflow_rank = top_data_df[top_data_df['domain']=='airlflow.com']['rank']
            f.write(airflow_rank)
        
        
def print_data(ds):
    with open('topchik10', 'r') as f:
        all_data = f.read()
    with open('long_name', 'r') as f:
        all_data_com = f.read()
    with open('airrank.csv', 'r') as f:
        all_data_com = f.read()
        
        
    date = ds

    print(f'Top domains in  for date {date}')
    print(all_data)

    print(f'Long domain name for date {date}')
    print(all_data_com)

    print(f'Position for air for date {date}')
    print(all_data_com)

default_args = {
    'owner': 'a.gerasimenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 10),
}
schedule_interval = '0 12 * * *'

dag = DAG('dag_gerasimenko', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='topchik10',
                    python_callable=topchik10,
                    dag=dag)

t3 = PythonOperator(task_id='long_name',
                        python_callable=long_name,
                        dag=dag)

t4 = PythonOperator(task_id='airrank',
                    python_callable=airrank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

