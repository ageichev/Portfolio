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
def get_top_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domains = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_10_domains = top_10_domains.value_counts().reset_index()['index'].head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def longest_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_dm = top_data_df.loc[top_data_df.domain.str.len() == top_data_df.domain.str.len().max()].sort_values(
        by='domain').head(1)
    longest_dm = longest_dm.domain[longest_dm.domain.str.len().idxmax()]
    with open('longest_dom.txt', 'w') as f:
        f.write(longest_dm)
        f.close()

# На каком месте находится домен airflow.com?
def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_pl = top_data_df.query('domain == "airflow.com"')
    airflow_pl = 'no such value in data frame' if airflow_pl.empty else str(airflow_pl['rank'].index[0])
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_pl)
        f.close()


def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        top_10_domains_data = f.read()
    with open('longest_dom.txt', 'r') as f:
        longest_dom_data = f.read()
    with open('airflow_rank.txt') as f:
        airflow_rank_data = f.read()

    date = ds

    print(f'Top domains for date {date}')
    print(top_10_domains_data)

    print(f'Longest domain for date {date}')
    print(longest_dom_data)

    print(f'Airflow domain rank for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'd-ivashkin-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=6),
    'start_date': datetime(2022, 8, 23),
}
schedule_interval = '5 16 * * *'

dag = DAG('first_dag_d-ivashkin-23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag)

t2_2 = PythonOperator(task_id='longest_dom',
                        python_callable=longest_dom,
                        dag=dag)

t2_3 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)