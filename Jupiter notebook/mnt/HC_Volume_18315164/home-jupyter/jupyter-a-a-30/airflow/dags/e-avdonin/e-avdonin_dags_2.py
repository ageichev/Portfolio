
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


def get_top10_zone():
    # Найти топ-10 доменных зон по численности доменов
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_domain_10_df = top_data_df.groupby('zone', as_index=False) \
        .agg({'domain':'count'}) \
        .rename(columns = {'domain':'domain_count'}) \
        .sort_values(by='domain_count', ascending = False)
    
    top_domain_10_df = top_domain_10_df.head(10)
    
    with open('top_10_domain_amount.csv', 'w') as f:
        f.write(top_domain_10_df.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_max_length_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x))
    max_len_domain_df = top_data_df.sort_values(by='domain_len', ascending = False).head(1)['domain']
    
    with open('max_len_domain_df.csv', 'w') as f:
        f.write(max_len_domain_df.to_csv(index=False, header=False))


# На каком месте находится домен airflow.com?         
def get_airflow_domain_rank():   
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')['rank']
    
    with open ('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domain_amount.csv', 'r') as f:
        top_domain_10_df = f.read()
    with open('max_len_domain_df.csv', 'r') as f:
        max_len_domain_df = f.read()
    date = ds
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()

    print(f'Top ten domains in the world on {date}')
    print(top_domain_10_df)

    print(f'The longest domain name on {date}')
    print(max_len_domain_df)
    
    print(f'airflow.com position on {date}')
    print(airflow_rank)


default_args = {
    'owner': 'e-avdonin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 7),
}
schedule_interval = '0 12 * * *'

eavdonin_dag = DAG('e-avdonin', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=eavdonin_dag)

t2 = PythonOperator(task_id='get_top10_zone',
                    python_callable=get_top10_zone,
                    dag=eavdonin_dag)

t3 = PythonOperator(task_id='get_max_length_domain',
                        python_callable=get_max_length_domain,
                        dag=eavdonin_dag)

t4 = PythonOperator(task_id='get_airflow_domain_rank',
                    python_callable=get_airflow_domain_rank,
                    dag=eavdonin_dag)
t5 = PythonOperator(task_id = 'print_data',
                    python_callable=print_data,
                    dag=eavdonin_dag)

t1 >> [t2, t3, t4] >> t5
