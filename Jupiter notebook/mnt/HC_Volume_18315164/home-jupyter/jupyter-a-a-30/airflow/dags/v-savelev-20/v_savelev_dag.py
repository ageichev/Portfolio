import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import re

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'



def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_zone_stat():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    df['dom_zone'] = df.domain.apply(lambda x: re.findall(r'\..*', x)[0].split('.')[-1])
    
    zone_top_10 =\
                    df\
                        .groupby('dom_zone', as_index=False)\
                        .agg({'domain': 'count'})\
                        .rename(columns={'domain': 'num'})\
                        .sort_values('num', ascending=False)\
                        .head(10)
    
    with open('zone_top_10.csv', 'w') as f:
        f.write(zone_top_10.to_csv(index=False, header=False))


def get_name_lenght():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    df['name_lenght'] = df.domain.apply(lambda x: len(x))

    max_len = df.name_lenght.max()

    max_lenght =\
                df\
                    .query('name_lenght == @max_len')\
                    .sort_values('domain')\
                    .head(1)
    
    with open('max_lenght.csv', 'w') as f:
        f.write(max_lenght.to_csv(index=False, header=False))
        

def get_position():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    if len(df.query('domain == "airflow.com"')) > 0:
        ans = f'''Airflow is on {df.query('domain == "airflow.com"')['rank'].to_list()[-1]} position'''
    else:
        ans = 'Airflow is not in list'
    
    with open('position.csv', 'w') as f:
        f.write(ans)

def print_data(ds):
    with open('zone_top_10.csv', 'r') as f:
        zone_data = f.read()
    with open('max_lenght.csv', 'r') as f:
        lenght_data = f.read()
    with open('position.csv', 'r') as f:
        position_data = f.read()   
    date = ds

    print(f'Top domains zones for date {date}')
    print(zone_data)

    print(f'Top domain name lenght for date {date}')
    print(lenght_data)
    
    print(f'For {date}')
    print(position_data)


default_args = {
    'owner': 'v.savelev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 1),
}
schedule_interval = '25 15 * * *'

dag = DAG('v.savelev_homework', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone_stat',
                    python_callable=get_zone_stat,
                    dag=dag)

t2_add = PythonOperator(task_id='get_name_lenght',
                        python_callable=get_name_lenght,
                        dag=dag)

t2_add_add = PythonOperator(task_id='get_position',
                        python_callable=get_position,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_add, t2_add_add] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_add)
#t1.set_downstream(t2_add_add)
#t2.set_downstream(t3)
#t2_add.set_downstream(t3)
#t2_add_add.set_downstream(t3)
