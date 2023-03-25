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


def get_stat_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['end_domain'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    
    top_data_top_10 = top_data_df.groupby('end_domain',as_index=False).agg({'rank':'count'}).rename(columns={'rank':'counts'}).\
    sort_values('counts', ascending=False).head(10)
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_stat_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x))
    top_len_top_1 = top_data_df[top_data_df.domain_len == top_data_df.domain_len.max()].sort_values('domain').head(1)
    with open('top_len_top_1.txt', 'w') as f:
        f.write(str(top_len_top_1['domain'].values[0]))

def get_stat_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_rank_by_name = top_data_df[top_data_df.domain == 'airflow.com']
    try:
        domain_rank_by_name = top_data_df.loc[top_data_df['domain'] == "airflow.com"].index.item()
    except:
        domain_rank_by_name = 'airflow.com is not in most popular domains'
    with open('domain_rank_by_name.txt', 'w') as f:
        f.write(domain_rank_by_name)
        
def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
        
    with open('top_len_top_1.txt', 'r') as f:
        top_1_len = f.read()
    with open('domain_rank_by_name.txt', 'r') as f:
        airflow_rank = f.read()

    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)
    
    print(f'Top 1 len domains for date {date}')
    print('len: ' + top_1_len)
    
    print(f'Rank of airflow.com for date {date}')
    print(airflow_rank)
    




default_args = {
    'owner': 't.zadornova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2021, 12, 12),
    'schedule_interval': '*/3 * * * *',
    'catchup': False
}

dag = DAG('top_stat_zadornova', default_args=default_args)

t1 = PythonOperator(task_id='get_data_tz',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_domain_tz',
                    python_callable=get_stat_domain,
                    dag=dag)

t2_len = PythonOperator(task_id='get_stat_len_tz',
                        python_callable=get_stat_len,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_stat_rank_tz',
                        python_callable=get_stat_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data_tz',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_len,t2_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
