import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

TOP_1M = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_FILE = 'top-1m.csv'

# Реализация DAGa на парадигме 2 Airflow
# Параметры DAGa
default_args = {
    'owner': 'a.novoselov-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 19),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def top_zones_range():

# Задаем Таски. Каждая таска - функция.

    @task()
    def get_data():
        top_doms = requests.get(TOP_1M, stream=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_FILE).decode('utf-8')
        return top_data


    @task()
    def get_top_zones(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
        top_zones = top_data_df\
            .groupby('domain_zone')\
            .agg({'domain':'count'})\
            .sort_values('domain', ascending=False)\
            .reset_index()\
            .head(10)
        return top_zones.to_csv(index=False)

    @task()
    def max_len_domain(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_df['len_domain'] = top_data_df['domain'].apply(lambda x: len(x))
        len_max_domain = top_data_df.sort_values(['len_domain', 'domain'], ascending=[False, True]).head(1)
        len_max_domain_name = len_max_domain['domain']
        return len_max_domain_name.to_csv(index=False)

    @task()
    def get_airflow_rank(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        airflow_rank = top_data_df.query('domain.str.contains("airflow.com")').reset_index()
        airflow_rank_num = airflow_rank['rank']
        return airflow_rank_num.to_csv(index=False)

    @task()
    def print_data(top_zones, len_max_domain_name, airflow_rank_num):

        context = get_current_context()
        date = context['ds']

        print(f'Top zones for date {date}')
        print(top_zones)

        print(f'The largest domain for date {date}')
        print(len_max_domain_name)

        print(f'For date {date}')
        print(f'The airflow.com`s rank is {airflow_rank_num}')

    top_data = get_data()
    top_zones = get_top_zones(top_data) 
    len_max_domain_name = max_len_domain(top_data)
    airflow_rank_num = get_airflow_rank(top_data)
    print_data(top_zones, len_max_domain_name, airflow_rank_num)

top_zones_range = top_zones_range()
