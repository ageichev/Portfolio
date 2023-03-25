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
        

def get_zones():
    
    """топ-10 доменных зон по численности доменов"""
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: '.' + x.split('.')[1])
    top_10_zones = pd.DataFrame(top_data_df['zone'].value_counts()[:10]).reset_index()
    
    with open('top_10_zones_echujumova.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))
        
        
def max_len_zone():
    
    '''домен с самым длинным именем
       (если их несколько, то взять только первый 
       в алфавитном порядке)'''
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: '.' + x.split('.')[1])
    top_data_df['len_zone'] = top_data_df['zone'].apply(lambda x: len(x))
    
    max_len_zone = list(top_data_df.query('len_zone == len_zone.max()')['zone'])
    zone = max_len_zone[0]

    for elem in max_len_zone:
        if zone >= elem:
            zone = elem
        else:
            continue
    
    with open('max_len_zone_echujumova.csv', 'w') as f:
        f.write(zone)
        
        
def get_airflow_rank():
    
    '''На каком месте находится домен airflow.com?'''
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')['rank']

    with open('airflow_rank_echujumova.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))  
        
        
        
def print_data(ds):
    with open('top_10_zones_echujumova.csv', 'r') as f:
        top_10_zones = f.read()
    with open('max_len_zone_echujumova.csv', 'r') as f:
        max_len_zone = f.read()
    with open('airflow_rank_echujumova.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top-10 domains zones on {date}')
    print(top_10_zones)

    print(f'Zone with maximal name lenght (1st in alphabet order) on {date}')
    print(max_len_zone)
    
    print(f'Airflow.com rank on {date}')
    print(airflow_rank)
    
    
default_args = {
    'owner': 'e-churjumova-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
}
schedule_interval = '30 12 * * *'

dag = DAG('top_10_echurjumova23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data_echurjumova23',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zones_echurjumova23',
                    python_callable=get_zones,
                    dag=dag)

t3 = PythonOperator(task_id='max_len_zone_echurjumova23',
                    python_callable=max_len_zone,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank_echurjumova23',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data_echurjumova23',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5