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


def get_top_10_domain_zones():
    df = pd.read_csv('top-1m.csv', header=None, index_col=0)
    df.rename(columns={0: 'rate', 1 : 'domain'}, inplace=True)
    df['name'] = df['domain'].str.split('.').str[0]
    df['domain_zone'] = df['domain'].str.split('.').str[1]
    domains = df.groupby('domain_zone', as_index=False).agg({'name' : 'count'}) \
    .sort_values('name', ascending=False) \
    .rename(columns={'name' : 'number'}).head(10)
    a = domains.domain_zone.to_list()
    b = domains.number.to_list()
    domains = dict(zip(a, b))
    with open('domains.txt', 'w') as f:
        for key, val in domains.items():
            f.write('{}:{}\n'.format(key,val))


def most_long_domain():
    df = pd.read_csv('top-1m.csv', header=None, index_col=0)
    df.rename(columns={0: 'rate', 1 : 'domain'}, inplace=True)
    df['name'] = df['domain'].str.split('.').str[0]
    df['length'] = df.name.apply(lambda x: len(x))
    most_long = df.sort_values(by = 'length', ascending=False).head(1).domain
    with open('most_long.txt', 'w') as f:
        f.write(most_long.to_list()[0])


def airflow_rate():
    df = pd.read_csv('top-1m.csv', header=None, index_col=0)
    df.rename(columns={0: 'index', 1 : 'domain'}, inplace=True)
    df['name'] = df['domain'].str.split('.').str[0]
    try:
        airflow_rate = str(df.query('name == "airflow.com"').index[0])
    except:
        airflow_rate = 'No such domen!'
    with open('airflow_rate.txt', 'w') as f:
        f.write(airflow_rate)
        
def log():
    with open("domains.txt") as file:
        a = file.read()
    with open("most_long.txt") as file:
        b = file.read()
    with open("airflow_rate.txt") as file:
        c = file.read()
    print(a)
    print(b + '\n')
    print(c)


default_args = {
    'owner': 'm.pozdeev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 30),
    }


schedule_interval = '0 16 * * *'

dag = DAG('m_pozdeev_23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id = 'get_data',
                    python_callable = get_data,
                    dag = dag)

t2 = PythonOperator(task_id ='get_top_10_domain_zones',
                    python_callable = get_top_10_domain_zones,
                    dag = dag)

t3 = PythonOperator(task_id = 'most_long_domain',
                        python_callable = most_long_domain,
                        dag = dag)
                        
t4 = PythonOperator(task_id='airflow_rate',
                    python_callable=airflow_rate,
                    dag=dag)

t5 = PythonOperator(task_id='log',
                    python_callable=log,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
