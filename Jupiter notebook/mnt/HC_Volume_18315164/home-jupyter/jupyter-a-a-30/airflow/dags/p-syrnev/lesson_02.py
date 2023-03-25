from unittest import result
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'
TOP_10_DOMAIN_ZONES = 'top-10-domain-zones.csv'
LONGEST_DOMAIN_NAME = 'longest-domain.txt'
AIRFLOW_RANK_NAME = 'airflow_rank.txt'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top10_domain_zones():
    # Найти топ-10 доменных зон по численности доменов
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_zones_df = df.domain.str.extract(r'.+(?P<domain_zone>\.\w+)')\
        .value_counts() \
        .sort_values(ascending=False) \
        .reset_index() \
        .rename(columns={0:'counts', 'counts':'domain_zone'}) \
        .head(10)
    top_domain_zones_df.to_csv(TOP_10_DOMAIN_ZONES, index=False, header=False)

def get_longest_name_domain():
    # Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_name'] = df.domain.str.extract(r'(?P<domain_name>\w+)\..+')
    
    max_len = df.domain_name.str.len().max()
    result = df[df.domain_name.str.len() == max_len].sort_values(by='domain_name').head(1).domain.iloc[0]

    with open(LONGEST_DOMAIN_NAME, 'w') as f:
        f.write(result)

def get_airflow_palce():
    # На каком месте находится домен airflow.com?
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    df_airflow = df[df.domain == 'airflow.com']

    result = 'airflow.com is not found'
    if df_airflow.shape[0] != 0:
        result = f'airflow.com take {df_airflow["rank"].values[0]} place';

    with open(AIRFLOW_RANK_NAME, 'w') as f:
        f.write(str(result))
    
def print_data(ds):
    with open(TOP_10_DOMAIN_ZONES, 'r') as f:
        top_10_domain_zones = f.read()

    with open(LONGEST_DOMAIN_NAME, 'r') as f:
        longest_domain = f.read()

    with open(AIRFLOW_RANK_NAME, 'r') as f:
        airflow_rank = f.read()
    date = ds

    result = f'''
___________
date {date}
1. Top 10 domains for date:
{top_10_domain_zones}

2. The longest domain name: {longest_domain}

3. airflow.com rank: {airflow_rank}
__________
    '''
    print(result)


default_args = {
    'owner': 'p-syrnev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 18),
}
schedule_interval = '0 10 * * *'

dag = DAG('p-syrnev-23-lesson-02', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_domain_zones',
                    python_callable=get_top10_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name_domain',
                    python_callable=get_longest_name_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_palce',
                    python_callable=get_airflow_palce,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t3)
#t1.set_downstream(t4)

#t2.set_downstream(t5)
#t3.set_downstream(t5)
#t4.set_downstream(t5)