from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

TOP_DOMAINS_DATA = 'tsiporin_top_domains.csv'
TOP_ZONE = 'tsiporin_top_zone.csv'
LONGEST_DOMAIN = 'tsiporin_longest_domain.txt'
AIRFLOW_RANK = 'tsiporin_airflow_rank.txt'


def task_download_top_domains():
    top_domains_url = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'

    pd.read_csv(top_domains_url, compression='zip', names=['rank', 'domain']) \
      .to_csv(TOP_DOMAINS_DATA, index=False)


def task_top_domain_zone():
    df = pd.read_csv(TOP_DOMAINS_DATA)
    df['zone'] = df.domain.str.extract(r'.+\.([a-z]+)$')

    df.groupby('zone', as_index=False) \
      .agg({'rank': 'count'}) \
      .rename(columns={'rank': 'quant'}) \
      .sort_values('quant', ascending=False) \
      .head(10) \
      .to_csv(TOP_ZONE, index=False)


def task_longest_domain():
    df = pd.read_csv(TOP_DOMAINS_DATA)

    df['domain_len'] = df.domain.str.len()
    max_domain_len = df.domain_len.max()

    longest_domain_name = df.query('domain_len == @max_domain_len') \
                            .sort_values('domain') \
                            .head(1).domain.values[0]

    with open(LONGEST_DOMAIN, 'w') as f:
        f.write(longest_domain_name)


def task_airflow_rank():
    df = pd.read_csv(TOP_DOMAINS_DATA)
    airflow_rank = df.query('domain == "airflow.com"')['rank'].values[0]

    with open(AIRFLOW_RANK, 'w') as f:
        f.write(airflow_rank)


def task_logger():
    top_zone = pd.read_csv(TOP_ZONE)

    with open(LONGEST_DOMAIN, 'r') as f:
        longest_domain_name = f.read()

    with open(AIRFLOW_RANK, 'r') as f:
        airflow_rank = f.read()

    print('10 наиболее крупных доменных зон:')
    print(top_zone)

    print(f'Самое длинное доменное имя: {longest_domain_name}')

    print(f'Домен airflow.com находится на {airflow_rank} месте в рейтинге')


def task_cleaner():
    delete_names = (TOP_DOMAINS_DATA, TOP_ZONE, LONGEST_DOMAIN, AIRFLOW_RANK)

    for name in delete_names:
        os.remove(name)


default_args = {
    'owner': 'a-tsiporin-31',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1),
}

schedule_interval = '0 12 * * *'

dag = DAG('a.tsiporin_lesson2_dag', default_args=default_args, schedule_interval=schedule_interval)

download_top_domains = PythonOperator(task_id='download_top_domains',
                                      python_callable=task_download_top_domains,
                                      dag=dag)

top_domain_zone = PythonOperator(task_id='top_domain_zone',
                                 python_callable=task_top_domain_zone,
                                 dag=dag)

longest_domain = PythonOperator(task_id='longest_domain',
                                python_callable=task_longest_domain,
                                dag=dag)

airflow_rank = PythonOperator(task_id='airflow_rank',
                              python_callable=task_airflow_rank,
                              dag=dag)

logger = PythonOperator(task_id='logger',
                        python_callable=task_logger,
                        dag=dag)

cleaner = PythonOperator(task_id='cleaner',
                         python_callable=task_cleaner,
                         dag=dag)

download_top_domains >> [top_domain_zone, longest_domain, airflow_rank] >> logger >> cleaner
