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

def get_top_10_domains_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domains_zone = top_data_df.groupby('domain_zone', as_index=False).agg({'rank': 'count'}).sort_values(by='rank', ascending=False).head(10)
    with open('top_10_domains_zone.csv', 'w') as f:
        f.write(top_10_domains_zone.to_csv(index=False, header=False))
        
def get_most_lenght_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['lenght_symbols'] = top_data_df['domain'].apply(lambda x: len(x))
    most_lenght_domain = top_data_df.sort_values(by='lenght_symbols', ascending=False)['domain'].head(1)
    with open('most_lenght_domain.csv', 'w') as f:
        f.write(most_lenght_domain.to_csv(index=False, header=False))
        
def get_airflow_com_number():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_com_number = top_data_df.query('domain == "airflow.com"')['rank']
    #if len(airflow_com_number) == 0:
    #    airflow_com_number = 'Domain airflow.com is not found'
    with open('airflow_com_number.csv', 'w') as f:
        f.write(airflow_com_number.to_csv(index=False, header=False))        


def print_data(ds):
    with open('top_10_domains_zone.csv', 'r') as f:
        top_10_domains_zone = f.read()
    with open('most_lenght_domain.csv', 'r') as f:
        most_lenght_domain = f.read()
    with open('airflow_com_number.csv', 'r') as f:
        airflow_com_number = f.read()
        
    date = ds
    

    print(f'Top 10 useful domains zone for date {date}')
    print(top_10_domains_zone)

    print(f'Most lenght name of domain for date {date}')
    print(most_lenght_domain)
    
    print(f'airflow.com number in database for date {date}')
    print(airflow_com_number)


default_args = {
    'owner': 'd-kuznetsov-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 17),    
}
schedule_interval = '0 10 * * *'

dag = DAG('d-kuznetsov-lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains_zone',
                    python_callable=get_top_10_domains_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_most_lenght_domain',
                    python_callable=get_most_lenght_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_com_number',
                    python_callable=get_airflow_com_number,
                    dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)