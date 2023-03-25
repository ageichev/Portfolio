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


def get_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone']=top_data_df['domain'].str.split('.').str[-1]    
    top_domain_10=top_data_df.groupby("domain_zone",       as_index=False).count().sort_values(by='domain', ascending=False)
    top_domain_10.rename(columns = {'domain' : 'count'}, inplace = True) 
    top_domain_10.head(10)
    with open('amount_domain_zone.csv', 'w') as f:
        f.write(amount_domain_zone.to_csv(index=False, header=False))


def get_max_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].str.split('.').str[-2]
    top_data_df['domain_name'].astype('str')
    top_data_domain['length_of_name']=top_data_domain['domain_name'].map(lambda x: len(x))
    top_data_domain.sort_values(by=['length_of_name'],ascending=False).head(1)
    with open('max_length_domain.csv', 'w') as f:
        f.write(max_length_domain.to_csv(index=False, header=False))

def get_name()
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].str.split('.').str[-2]
    airflow=top_data_df.query('domain_name=="airflow"')
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))
        
def print_data(ds):
    with open('amount_domain_zone.csv', 'r') as f:
        all_data = f.read()
    with open('max_length_domain.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow.csv', 'r') as f:
        all_data_name = f.read()    
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)
-
    print(f'The longest domain name  {date}')
    print(all_data_len)
       
    print(f'The rank of airflow.com {date}')
    print(all_data_name)

default_args = {
    'owner': 'a-antonov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 15, 1),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-antonov-28', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_zone',
                    python_callable=get_zone,
                    dag=dag)

t2 = PythonOperator(task_id='get_name',
                    python_callable=get_name,
                    dag=dag)

t2_com = PythonOperator(task_id='get_max_length',
                        python_callable=get_max_length,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_com] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

