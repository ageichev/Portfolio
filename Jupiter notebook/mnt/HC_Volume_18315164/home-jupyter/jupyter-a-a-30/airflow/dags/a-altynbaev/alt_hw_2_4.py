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
def get_top_10_zones():
    top_zones_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_zones_df['zone'] = top_zones_df.domain.str.split('.').str[-1]
    top_zones_10 = top_zones_df.groupby('zone').agg({'domain': 'count'}).reset_index() \
                        .sort_values('domain', ascending = False).head(10)
    with open('top_zones_10.csv', 'w') as f:
        f.write(top_zones_10.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].str.len()
    longest = top_data_df.sort_values(['length', 'domain'], ascending=[False, True]).head(2)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))
        
# На каком месте находится домен airflow.com?
def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df[top_data_df.domain == 'airflow.com'].shape[0] != 0:
        airflow_df = top_data_df[top_data_df.domain == 'airflow.com']['rank']
    else:
        airflow_df = pd.DataFrame({'col_1': ["It seems like airflow.com isn't in the list"]})
    with open('airflow_df.csv', 'w') as f:
        f.write(airflow_df.to_csv(index=False, header=False))
        

def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        zones_data = f.read()
    with open('longest_domain.csv', 'r') as f:
         longest_data = f.read()
    with open('airflow_df.csv', 'r') as f:
         airflow_data = f.read()        
        
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(zones_data)
    
    print(f'Domain with longest name for date {date}')
    print(longest_data)

    print(f'Airflow.com rank for date {date}')
    print(airflow_data)
    

default_args = {
    'owner': 'a.altynbaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 29),
}
schedule_interval = '01 08 * * *'

dag = DAG('a-altynbaev', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
