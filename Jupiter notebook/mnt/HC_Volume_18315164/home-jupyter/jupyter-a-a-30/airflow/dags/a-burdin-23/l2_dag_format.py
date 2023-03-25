import requests
import pandas as pd
import numpy as np
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
        
def get_stat_top10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_ends'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_data_top_10 = top_data_df.groupby(["domain_ends"], as_index=False).agg({"domain":"count"}).sort_values("domain", ascending=False).head(10)
    with open('top_data_top10_domains.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))
        
def get_stat_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    lengths = top_data_df["domain"].str.len()
    argmax = np.where(lengths == lengths.max())[0]
    top_data_longest_domain = top_data_df.iloc[argmax]
    with open('top_data_longest_domain.csv', 'w') as f:
        f.write(top_data_longest_domain.to_csv(index=False, header=False))

def get_stat_index_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    index_domain = pd.DataFrame(top_data_df[top_data_df.domain.str.contains("airflow.com")].index.values.astype(int))
    with open('top_data_index_domain.csv', 'w') as f:
        f.write(index_domain.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top10_domains.csv', 'r') as f:
        all_data_top10_domains = f.read()
    with open('top_data_longest_domain.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('top_data_index_domain.csv', 'r') as f:
        all_data_index_domain = f.read()
    date = ds

    print(f' Топ-10 доменных зон по численности доменов {date}')
    print(all_data_top10_domains)

    print(f' Домен с самым длинным именем {date}')
    print(all_data_longest_domain)
    
    print(f' Индекс домена airflow.com {date}')
    print(all_data_index_domain)

default_args = {
    'owner': 'a.burdin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
}
schedule_interval = '0 8 * * *'

dag_burdin = DAG('burdin_28082022_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_burdin)

t2_top10 = PythonOperator(task_id='get_stat_top10_domains',
                    python_callable=get_stat_top10_domains,
                    dag=dag_burdin)

t2_longest_domain = PythonOperator(task_id='get_stat_longest_domain',
                        python_callable=get_stat_longest_domain,
                        dag=dag_burdin)

t2_index_domain = PythonOperator(task_id='get_stat_index_domain',
                        python_callable=get_stat_index_domain,
                        dag=dag_burdin)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_burdin)

t1 >> [t2_top10, t2_longest_domain, t2_index_domain] >> t3


print("That's all!")
