import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'a.sahabutdinov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 28),
}

@dag(default_args=default_args, catchup=False)
def top_10_airflow_2_by_Aidar():
    @task
    def get_data():
        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
        return top_data
    
    @task
    def get_stat_ru(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_top_10_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')] 
        top_data_top_10_ru = top_data_top_10_ru.head(10)
        return top_data_top_10_ru.to_csv(index=False)    

    @task
    def get_stat_com(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_top_10_com = top_data_df[top_data_df['domain'].str.endswith('.com')] 
        top_data_top_10_com = top_data_top_10_com.head(10)
        return top_data_top_10_com.to_csv(index=False)

    @task
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

top_10_airflow_2_by_Aidar = top_10_airflow_2_by_Aidar()
