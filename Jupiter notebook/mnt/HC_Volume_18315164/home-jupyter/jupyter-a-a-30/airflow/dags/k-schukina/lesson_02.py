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


def get_top_domain_ksch():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_count_domain_10 = top_data_df['domain'].value_counts().sort_values(ascending=False).head(10).reset_index()
    with open('top_count_domain_10.csv', 'w') as f:
        f.write(top_count_domain_10.to_csv(index=False, header=False))


def get_longest_domain_ksch():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df['domain'].apply(lambda x: len(x.split('.')[0]))
    max_len = top_data_df.sort_values(by='domain_len', ascending=False)['domain_len'].max()
    top_len = top_data_df.loc[top_data_df['domain_len'] == max_len]
    with open('top_len.csv', 'w') as f:
        f.write(top_len.to_csv(index=False, header=False))
        
def airflow_place_ksch():
    airflow_search = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_found = airflow_search[airflow_search['domain'] == 'airflow.com']
    with open('airflow_search.csv', 'w') as f:
        f.write(airflow_found.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_count_domain_10.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('top_len.csv', 'r') as f:
        best_longest = f.read()
    with open('airflow_search.csv', 'r') as f:
        airflow_search = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domain_zone)

    print(f'The longest domain for date {date}')
    print(best_longest)
    
    print(f'The airflow place for date {date}')
    if len(airflow_search) > 0:
        print(airflow_search)
    else:
        print('No airflow.com domain found')



default_args = {
    'owner': 'k-schukina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 23, 2),
}
schedule_interval = '0 10 * * *'

dag = DAG('airflow_homework_k-schukina', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_rate',
                    python_callable=get_domain_rate,
                    dag=dag)

t2_top_len = PythonOperator(task_id='get_top_len',
                        python_callable=get_top_len,
                        dag=dag)

t2_airflow_rate = PythonOperator(task_id='airflow_place',
                        python_callable=airflow_place,
                        dag=dag)                        

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_top_len, t2_airflow_rate] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)