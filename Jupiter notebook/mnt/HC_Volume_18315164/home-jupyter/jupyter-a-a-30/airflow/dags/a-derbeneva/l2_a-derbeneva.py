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

# Найти топ-10 доменных зон по численности доменов
def get_stat_domain_zone():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['zone'] = top_doms.domain.str.split('.').str[-1]
    top_data_top_10 = top_doms.zone.value_counts().head(10).reset_index().rename(columns={'index':'zone', 'zone':'count'})
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_stat_domain_zone_max_len():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['len_domain']= top_doms.domain.str.len()
    top_data_max_len = top_doms.sort_values('len_domain', ascending= False).head(1)
    with open('top_data_max_len.csv', 'w') as f:
        f.write(top_data_max_len.to_csv(index=False, header=False))
        
# На каком месте находится домен airflow.com?
def get_stat_domain_place():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['len_domain'] = top_doms.domain.str.len()
    top_doms = top_doms.sort_values('len_domain', ascending= False).reset_index()
    domen_place = top_doms.query("domain == 'airflow.com'").reset_index()
    domen_place = domen_place[['domain', 'level_0']].rename(columns = {'level_0':'place'})
    if domen_place.empty:
        result = 'Not in the list'
    else:
        result = domen_place.place
    print(result)
    with open('domen_place.txt', 'w') as f:
        f.write(str(result))


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('top_data_max_len.csv', 'r') as f:
        max_len = f.read()
    with open('domen_place.txt', 'r') as f:
        domen_place = f.read()
    date = ds

    print(f'Top 10 domains zone for date {date}')
    print(top_10)

    print(f'Longest domain name for date {date}')
    print(max_len)
    
    print(f'"airflow.com" in long domain name rating  for date {date}')
    print(domen_place)


default_args = {
    'owner': 'a-derbeneva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 1, 24),
}
schedule_interval = '15 02 * * *'

a_der_dag = DAG('a-derbeneva', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=a_der_dag)

t2 = PythonOperator(task_id='get_stat_domain_zone',
                    python_callable=get_stat_domain_zone,
                    dag=a_der_dag)

t2_max_len = PythonOperator(task_id='get_stat_domain_zone_max_len',
                        python_callable=get_stat_domain_zone_max_len,
                        dag=a_der_dag)

t2_domain_place = PythonOperator(task_id='get_stat_domain_place',
                    python_callable=get_stat_domain_place,
                    dag=a_der_dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=a_der_dag)

t1 >> [t2, t2_max_len, t2_domain_place] >> t3