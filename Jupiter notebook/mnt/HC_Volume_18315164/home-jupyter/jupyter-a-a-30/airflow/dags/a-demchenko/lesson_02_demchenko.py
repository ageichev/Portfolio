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

#Найти топ-10 доменных зон по численности доменов

def get_top_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df['domain'].str.split('.').str[-1]
    df = (
        df
            .groupby('zone', as_index=False)
            .agg({'domain': 'count'}).sort_values(by='domain', ascending=False).head(10)['zone']
    )
    df.reset_index(drop=True, inplace=True)
    with open('count_domain_in_zone.csv', 'w') as f:
        f.write(df.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)

def get_longest_domain():
    top_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    s = top_domains_df.domain.str.len().sort_values(ascending=False).index
    df = top_domains_df.reindex(s)
    df = df.reset_index(drop=True)
    with open('longest_domain.csv', 'w') as f:
        f.write(df.iloc[0].domain)


# На каком месте находится домен airflow.com?   

def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    rank_airflow = str(top_data_df['rank'][top_data_df.domain.str.contains('airflow')].iloc[0])
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow)

        
        
def print_data(ds):  # передаем глобальную переменную airflow
    with open('count_domain_in_zone.csv', 'r') as f:
        count_domain_file = f.read()
    with open('longest_domain.csv', 'r') as f:
        max_length_file = f.read()
    with open('rank_airflow.csv', 'r') as f:
        airflow_rank_file = f.read()
    date = ds
    print(f'Top 10 domains for date {date} is')
    print(count_domain_file)
    print(f'The longest domain name for date {date} is')
    print(max_length_file)
    print(f'The rank of airflow.com for {date} is')
    print(airflow_rank_file)


default_args = {
    'owner': 'a-demchenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 19),
    'schedule_interval': '0 1 * * *'
}


dag = DAG('a_demchenko_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain',
                    python_callable=get_top_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

