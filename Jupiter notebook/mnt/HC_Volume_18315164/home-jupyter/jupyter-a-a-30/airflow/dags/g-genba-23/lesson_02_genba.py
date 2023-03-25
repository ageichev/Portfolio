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
        
        
        
def get_top():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_zones = top_data_df
    top_10_zones['zone'] = top_10_zones.domain.apply(lambda x: x.rsplit('.')[1])
    top_10_zones = top_10_zones.groupby('zone', as_index=False)\
                .agg({'domain':'count'})\
                .sort_values('domain', ascending=False)\
                .head(10)\
                .reset_index()\
                .zone
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

        
def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_df['len_domain_name'] = top_data_df['domain_name'].apply(len)

    df_long_domain = top_data_df.query('len_domain_name == len_domain_name.max()').sort_values(by = 'domain_name')
    long_domain = df_long_domain['domain_name'].values[0]
    
    with open('long_domain.txt', 'w') as f :
        f.write(long_domain)
        
def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    if top_data_df.query("domain == 'airflow.com'").shape[0] != 0:
        rank_airflow = top_data_df.query("domain == 'airflow.com'")
    else:
        rank_airflow = pd.DataFrame({'rank': 'not found', 'domain': ['airflow.com']})
        
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False))
        


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('long_domain.txt', 'r') as f:
        long_domain = f.read()
    with open('rank_airflow.csv', 'r') as f:
        rank_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_zones)

    print(f'Longest domain for date {date}')
    print(long_domain)
    
    print(f'Rank airflow.com for date {date}')
    print(rank_airflow)


default_args = {
    'owner': 'g.genba',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 27)
}
schedule_interval = '0 12 * * *'


dag = DAG('genba_domen_info', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top',
                    python_callable=get_top,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_domain',
                    python_callable=get_long_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                    python_callable=get_rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
