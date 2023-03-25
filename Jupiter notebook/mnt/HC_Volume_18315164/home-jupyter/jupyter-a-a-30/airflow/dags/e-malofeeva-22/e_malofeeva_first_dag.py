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


def get_top_10_domain_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    df_top_10_domain_zone = df.domain_zone.value_counts().head(10)
    with open('df_top_10_domain_zone.csv', 'w') as f:
        f.write(df_top_10_domain_zone.to_csv(header=False))


def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['len_name'] = df.domain.apply(lambda x : len(x))
    df = df.sort_values('len_name', ascending = False)
    max_domain = df.iloc[0]
    with open('max_domain.csv', 'w') as f:
        f.write(max_domain.to_csv(index=False, header=False))
        

def get_airflow_index():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_index = df.index[df.domain == 'airflow.com'].tolist()
    if len(airflow_index) > 0:
        df_airflow_index = pd.DataFrame(airflow_index, columns=["airflow_index"])
        df_airflow_index.to_csv('airflow_index.csv', index = False)
    else:
        s = ['None']
        import  csv
        with open("airflow_index.csv","w") as f:
            wr = csv.writer(f)
            wr.writerow(s)
        

def print_data(ds):
    with open('df_top_10_domain_zone.csv', 'r') as f:
        df_top_10_domain_zone = f.read()
    with open('max_domain.csv', 'r') as f:
        max_domain = f.read()
    with open('airflow_index.csv', 'r') as f:
        airflow_index = f.read()
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(df_top_10_domain_zone)

    print(f'Longest domain for date {date}')
    print(max_domain)
    
    print(f'Airflow rank for date {date}')
    print(airflow_index)


default_args = {
    'owner': 'e.malofeeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 31),
}
schedule_interval = '0 8 * * *'

dag = DAG('e_malofeeva_airflow2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_airflow_index',
                        python_callable=get_airflow_index,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_long, t2_rank] >> t3