import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag,task


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

def top_10_airflow_2_d_korzinin():
    @task()
    def get_data():
        # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
        top_doms = pd.read_csv(TOP_1M_DOMAINS)
        top_data = top_doms.to_csv(index=False)
        return top_data

    @task()
    def get_stat_ru(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_top_10_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        top_data_top_10_ru = top_data_top_10_ru.head(10)
        return top_data_top_10_ru.to_csv(index = False)

    @task()
    def get_stat_com(top_data):
        top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        top_data_top_10_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        top_data_top_10_com = top_data_top_10_com.head(10)
        return top_data_top_10_com.to_csv(index = False)

    @task()
    def print_data(top_data_top_10_ru, top_data_top_10_com):

        date = ''

        print(f'Top domains in .RU for date {date}')
        print(top_data_top_10_ru)

        print(f'Top domains in .COM for date {date}')
        print(top_data_top_10_com)
        
    top_data = get_data()
    top_data_top_10_ru = get_stat_ru(top_data)
    top_data_top_10_com = get_stat_com(top_data)
    print_data(top_data_top_10_ru, top_data_top_10_com)

top_10_airflow_2_d_korzinin = top_10_airflow_2_d_korzinin()