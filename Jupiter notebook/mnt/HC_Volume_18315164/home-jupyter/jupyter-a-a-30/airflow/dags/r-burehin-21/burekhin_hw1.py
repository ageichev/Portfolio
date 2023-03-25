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

def get_domen_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domen_10 = top_data_df.domain.str.split('.', expand=True)[1].value_counts().head(10)
    with open('top_domen_10.csv', 'w') as f:
        f.write(top_domen_10.to_csv( header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain = \
    (top_data_df
     .assign(
     domain_len = lambda x: x.domain.apply(len)
     )
     .sort_values(['domain_len', 'domain'], ascending = [False, False])
     .head(1)
     .loc[:,['domain','domain_len' ]]
    )
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv( header=True, index = False))

def place_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = \
    (top_data_df
     .assign(
     domain_len = lambda x: x.domain.apply(len)
     )
     .sort_values(['domain_len', 'domain'], ascending = [False, False])
     .assign(
         place = lambda x: range(1,len(x)+1)
     )
     .query("domain == 'airflow.com'")
    )
    with open('place_airflow.csv', 'w') as f:
        f.write(airflow.to_csv( header=True, index = False))
        
        
def print_data(ds):
    top_domen_10 = pd.read_csv('top_domen_10.csv', header=None)
    longest_domain = pd.read_csv('longest_domain.csv')
    place_airflow = pd.read_csv('place_airflow.csv')
        
    date = ds

    print(f'Top domains for date {date}')
    print(top_domen_10[0])
    
    print(f'Domain with longest name {date}')
    print(longest_domain.domain[0])
    
    print(f'Airflow place {date}')
    if place_airflow.shape[0] == 0:
        print('no data about - airflow.com')
    else:
        print('place_airflow.place[0]')


default_args = {
    'owner': 'r-burehin-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 26),
}
schedule_interval = '0 17 * * *'

dag = DAG('burekhin_top_10_domain_v3', 
          default_args=default_args, 
          schedule_interval=schedule_interval, 
          tags=["bur_train"]
         )

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_domain = PythonOperator(task_id='get_domen_10',
                    python_callable=get_domen_10,
                    dag=dag)

t2_longest_domain = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t2_airflow = PythonOperator(task_id='place_airflow',
                    python_callable=place_airflow,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_domain,t2_longest_domain, t2_airflow] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)