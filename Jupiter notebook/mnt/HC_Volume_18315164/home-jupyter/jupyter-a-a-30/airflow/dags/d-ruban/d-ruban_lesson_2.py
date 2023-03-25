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


def get_top_domain_zone():
    top_data_df =pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.apply(lambda x: x.split('.')[1])
    top_domain_zone = top_data_df.domain_zone.value_counts().head(10)
    with open('top_domain_zone.csv', 'w') as f:
        f.write(top_domain_zone.to_csv(index=False, header=False))

def get_max_length_domain():
    top_data_df_2 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df_2['length'] = top_data_df_2.domain.str.len()
    max_length = top_data_df_2.sort_values('length', ascending = False).head(1).iat[0,2]
    max_length_domain = top_data_df_2.query('length == @max_length').sort_values('domain').head(1)
    with open('max_length_domain.csv', 'w') as f:
        f.write(max_length_domain.to_csv(index=False, header=False))


def airflow_in_rank():
    top_data_df_3 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        try:
            top_data_df_3.query('domain == "airflow.com"').iat[0,0]
        except:
            rank = "airflow.com is not in this rank"
        else:
            rank = top_data_df_3.query('domain == "airflow.com"'.iat[0,0]
    rank                              
    with open('airflow_in_rank.csv', 'w') as f:
        f.write(airflow_in_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_domain_zone.csv', 'r') as f:
        top_domain_zone = f.read()
    with open('max_length_domain.csv', 'r') as f:
        max_length_domain = f.read()
    with open('airflow_in_rank.csv', 'r') as f:
        airflow_in_rank = f.read()
    
    date = ds

    print(f'Top domain zones {date}')
    print(top_domain_zone)

    print(f'Max length domain {date}')
    print(max_length_domain)
    
    print(f'Airflow.com in rank {date}')
    print(airflow_in_rank)


default_args = {
    'owner': 'd-ruban',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
}
schedule_interval = '30 8 * * *'


dag = DAG('d-ruban_lesson_2', default_args=default_args, schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain_zone',
                    python_callable=get_top_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length_domain',
                        python_callable=get_max_length_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_in_rank',
                        python_callable=airflow_in_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> t2 >> t3 >> t4 >> t5

