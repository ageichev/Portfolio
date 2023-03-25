import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat_top_10_dom():
    data_domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_domains['domain_zone'] = data_domains.domain.str.split('.').str[-1]
    top_10_zones = data_domains.groupby('domain_zone', as_index=False) \
                               .agg({'domain': 'count'}) \
                               .sort_values('domain', ascending=False) \
                               .rename(columns={'domain': 'count'}) \
                               .reset_index(drop=True) \
                               .head(10) 
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))


def get_stat_longest_dom():
    data_domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_domains['len_domain'] = data_domains.domain.sort_values(ascending=False).str.len()
    max_domain = data_domains.len_domain.max()
    longest_site_names = data_domains.query('len_domain == @max_domain') \
                               .reset_index(drop=True) \
                               .sort_values('domain', ascending=False) \
                               .head(1)
    with open('longest_site_names.csv', 'w') as f:
        f.write(longest_site_names.to_csv(index=False, header=False))
        

def get_stat_airflow_place():
    data_domains = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_place = data_domains.query('domain == "airflow.com"').reset_index(drop=True)
    with open('airflow_place.csv', 'w') as f:
        f.write(airflow_place.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    top_10_zones = pd.read_csv('top_10_zones.csv', names=['zone', 'count_domains'], index_col = False)
    longest_site_names = pd.read_csv('longest_site_names.csv', names=['place', 'domain', 'len'], index_col = False)
    airflow_place = pd.read_csv('airflow_place.csv', names=['place', 'domains'], index_col = False)
    date = ds

    print(f'Top 10 domain zones for date {date} are: {top_10_zones}')

    print(f'The longest domain for date {date} is: {longest_site_names.domain}')
    
    print(f'The place of airflow.com for date {date} is: {airflow_place.place}')


default_args = {
    'owner': 'a-yushkevich',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 2, 25),
}
schedule_interval = '0 13 * * *'

dag = DAG('a-yushkevich_dag_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_stat_top_10_dom',
                    python_callable=get_stat_top_10_dom,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_stat_longest_dom',
                    python_callable=get_stat_longest_dom,
                    dag=dag)

t2_airflow = PythonOperator(task_id='get_stat_airflow_place',
                        python_callable=get_stat_airflow_place,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_longest, t2_airflow] >> t3


#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
