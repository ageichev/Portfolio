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

# Найти топ-10 доменных зон по численности доменов
def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(lambda x: len(x)) # создание колонки с длиной имени домена
    max_len = top_data_df.len.max() # запомнили наибольшую длину имени домена
    longest_domain = top_data_df.loc[top_data_df.len == max_len].sort_values('domain').head(1).domain.values[0] # отобрали домены с самыми длинными именами, отсортировали и взяли первое
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)

# На каком месте находится домен airflow.com?
def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        rank_airflow = top_doms.query('domain == "airflow.com"')['rank'].values[0]
    except:
        rank_airflow = 'There is not "airflow.com" in data'
    with open('rank_airflow.txt', 'w') as f:
        f.write(rank_airflow)

def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10_domains = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('rank_airflow.txt', 'r') as f:
        rank_airflow = f.read()
    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_10_domains)

    print(f'The longest domain name for date {date}')
    print(longest_domain)
    
    print(f'The rank of airflow.com for date {date}')
    print(rank_airflow)


default_args = {
    'owner': 'm.gareeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 19),
}
schedule_interval = '00 14 * * *'

dag = DAG('m.gareeva_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3] >> t4


