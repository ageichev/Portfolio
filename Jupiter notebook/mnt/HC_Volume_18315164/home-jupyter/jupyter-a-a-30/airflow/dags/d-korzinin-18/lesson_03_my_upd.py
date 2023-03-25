import requests
import pandas as pd
import numpy as np

from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag,task
from airflow.operators.python import get_current_context


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'd-korzinin-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 14),
}

@dag(default_args=default_args, catchup = False)

def top_10_airflow_2_d_korzinin_upd():
    @task(retries = 3)
    def get_data():
        # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
        top_doms = pd.read_csv(TOP_1M_DOMAINS)
        top_data = top_doms.to_csv(index=False)
        return top_data

    @task(retries = 4, retry_delay = timedelta(10))
    def get_table_ru(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        return top_data_ru.to_csv(index = False)
    
    @task
    def get_stat_ru(top_data_ru):
        ru_df = pd.read_csv(StringIO(top_data_ru))
        ru_avg = ru_df['rank'].aggregate(np.mean)
        ru_median = ru_df['rank'].aggregate(np.median)
        return {'ru_avg': ru_avg, 'ru_median': ru_median}
    

    @task()
    def get_table_com(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        return top_data_com.to_csv(index = False)
    
    @task
    def get_stat_com(top_data_com):
        com_df = pd.read_csv(StringIO(top_data_com))
        com_avg = com_df['rank'].aggregate(np.mean)
        com_median = com_df['rank'].aggregate(np.median)
        return {'com_avg': com_avg, 'com_median': com_median}
    

    @task()
    def print_data(ru_stat, com_stat):

        context = get_current_context()
        date = context['ds']

        ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median'],
        com_avg, com_median = com_stat['com_avg'], com_stat['com_median']
        
        
        print(f'''Data from .RU for date {date}
                    Avg rank {ru_avg}
                    Median rank {ru_median}''')
        
        print(f'''Data from .COM for date {date}
                    Avg rank {com_avg}
                    Median rank {com_median}''')

        
    top_data = get_data()
    top_data_ru = get_table_ru(top_data)
    ru_stat = get_stat_ru(top_data_ru)
    top_data_com = get_table_com(top_data)
    com_stat = get_stat_com(top_data_com)
    print_data(ru_stat, com_stat)


top_10_airflow_2_d_korzinin_upd = top_10_airflow_2_d_korzinin_upd()