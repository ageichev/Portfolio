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


# Найти топ-10 доменных зон по численности доменов
def get_top10_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[1]
    top10_domain = top_data_df.groupby('domain_zone', as_index=False) \
        .agg({'domain': 'count'}) \
        .rename(columns={'domain': 'count'}) \
        .sort_values('count', ascending=False) \
        .head(10)
    with open('top10_domain.csv', 'w') as f:
        f.write(top10_domain.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_top_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df['domain'].str.split('.').str[0].str.len()
    top_len = top_data_df.sort_values(['len_domain', 'domain'], ascending=[False, True]).reset_index(drop=True).shift()[
              1:]
    top_1_len = top_len.head(1)
    # top_data_df.domain.str.contains('airflow', case=True).to_frame().query("website == True")
    # top_data_df.iloc[658860]
    # проверила еще таким путем.есть общущение, что не все данные подгружаются top_data_df[top_data_df.domain.str.startswith('airflow')]
    # c airflow есть только 1 сайт - airflowacademy.com
    top_airflow = top_len[top_len.domain == 'airflowacademy.com'].index[0]
    with open('top_len.csv', 'w') as f:
        f.write(top_len.to_csv(index=False, header=False))


def print_data(ds):
    with open('top10_domain.csv', 'r') as f:
        data_domain_zone = f.read()
    with open('top_len.csv', 'r') as f:
        data_len = f.read()
    date = ds

    print(f'Top domain zone are {date}')
    print(data_domain_zone)

    print(f'Top domains by lenght {date}')
    print(data_len)


default_args = {
    'owner': 'm-fazylova-17',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 4, 5),
}
schedule_interval = '30 9 * * *'

dag = DAG('dag_domains_stats_fazylove', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_domain = PythonOperator(task_id='get_top10_domain',
                           python_callable=get_data,
                           dag=dag)

t2_len = PythonOperator(task_id='get_top_len',
                        python_callable=get_data,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_domain, t2_len] >> t3

