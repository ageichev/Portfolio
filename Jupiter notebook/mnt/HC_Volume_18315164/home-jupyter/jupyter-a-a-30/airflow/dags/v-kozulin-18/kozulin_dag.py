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


def get_top_dom_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'url'])
    top_data_df['domain'] = top_data_df.url.apply(lambda x: x.split('.')[1])
    top_10_dom_zone = top_data_df.groupby('domain', as_index=False).agg({'url': 'count'})
    top_10_dom_zone = top_10_dom_zone.sort_values('url', ascending=False).head(10)
    with open('top_10_dom_zone.csv', 'w') as f:
        f.write(top_10_dom_zone.to_csv(index=False, header=False))


def get_longest_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'url'])
    top_data_df['domain_len'] = top_data_df.url.apply(lambda x: len(x))
    longest_dom = top_data_df.loc[top_data_df.domain_len == max(top_data_df.domain_len)].sort_values('url').head(1)
    name = ''.join(longest_dom.url.to_list())
    long_name = longest_dom.domain_len.to_list()[0]
    ans = f'Домен с самым длинным именем это {name},\nвключает в себя {long_name} символов'
    with open('longest_dom.txt', 'w') as f:
        f.write(ans)
        
def get_air_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'url'])
    air_rank = top_data_df.query('url == "airflow.com"')
    if air_rank.shape[0] == 0:
        air_rank = 'Нет такого домена в топ 1кк('
    else:
        air_rank = f'Домен airflow.com имеет ранг {air_rank.rank.to_list()[0]} '
    with open('air_rank.txt', 'w') as f:
        f.write(air_rank)


def print_data(ds):
    with open('top_10_dom_zone.csv', 'r') as f:
        top_10_dom_zone = f.read()
    with open('longest_dom.txt', 'r') as f:
        longest_dom = f.read()
    with open('air_rank.txt', 'r') as f:
        air_rank = f.read()
    date = ds
    
    print(f'Интересности для даты {date}')
    print('Топ-10 доменных зон по численности доменов')
    print(top_dom_zone)

    print(longest_dom)
    
    print(air_rank)


default_args = {
    'owner': 'v.kozulin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 12),
}
schedule_interval = '0 12 * * *'

dag = DAG('kozulin_interesting_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_dom_zone',
                    python_callable=get_top_dom_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_dom',
                        python_callable=get_longest_dom,
                        dag=dag)

t4 = PythonOperator(task_id='get_air_rank',
                        python_callable=get_air_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
