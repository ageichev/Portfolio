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

def get_stat_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['adres', 'zone'], sep='.')
    top_data_zone_10 = top_data_df.groupby('zone', as_index=False)\
        .agg({'adres':'count'})\
        .rename(columns={'adres':'count'})\
        .sort_values('count', ascending=False)\
        .head(10)
    with open('top_data_zone_10.csv', 'w') as f:
        f.write(top_data_zone_10.to_csv(index=False, header=False))

def get_stat_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    z =[]
    for i in range(len(top_data_df)):
        z.append(len(top_data_df['domain'][i]))
    top_data_df['lenght']=z
    top_data_len_10 = top_data_df.sort_values('lenght', ascending=False).head(10)
    with open('top_data_len_10.csv', 'w') as f:
        f.write(top_data_len_10.to_csv(index=False, header=False))

def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df[top_data_df['domain']=='airflow.com'].empty == True:
        airfloy = {'There is no airflow.com domain in data'}
        top_data_airfloy = pd.DataFrame(data=airfloy)
    else:
        top_data_airfloy = top_data_df[top_data_df['domain']=='airflow.com']
    with open('top_data_airfloy.csv', 'w') as f:
        f.write(top_data_airfloy.to_csv(index=False, header=False))

def print_data():
    with open('top_data_zone_10.csv', 'r') as f:
        all_data_zone = f.read()
    with open('top_data_len_10.csv', 'r') as f:
        all_data_len = f.read() 
    with open('top_data_airfloy.csv', 'r') as f:
        all_data_airflow = f.read() 
    print('Top zone of domains ')
    print(all_data_zone)
    
    print('Top domains lenght')
    print(all_data_len)
    
    print(all_data_airflow)    

default_args = {
    'owner': 'e-jankovenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16),
}
schedule_interval = '30 09 * * *'

dag = DAG('e-jankovenko-top-10', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_zone',
                    python_callable=get_stat_zone,
                    dag=dag)

t2_len = PythonOperator(task_id='get_stat_len',
                        python_callable=get_stat_len,
                        dag=dag)


t2_air = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_len, t2_air] >> t3
