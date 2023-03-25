import requests
import pandas as pd
from zipfile import ZipFile
from datetime import datetime, timedelta
from io import BytesIO, StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {'owner': 'p-gordejchuk',
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 2, 13),
                'schedule_interval': '*/1  * * *'}


@dag(default_args=default_args, catchup=False)
def top_10_airflow_pg() -> str:
    @task(retries=4, retry_delay=timedelta(10))
    def get_data():
        req_doms = requests.get(TOP_1M_DOMAINS, stream=True)
        zip_doms = ZipFile(BytesIO(req_doms.content))
        data_body: str = zip_doms.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
        data: str = "rank,domain\n" + data_body
        return data

    @task()
    def filter_data_ru(data) -> pd.Series:
        data_df = pd.read_csv(StringIO(data))
        data_ru = (
            data_df
            [data_df['domain'].str.endswith('.ru')]
        )
        return data_ru.to_csv(index=False)

    @task()
    def calc_stat_ru(data_ru_df) -> dict:
        data_ru_df = data_ru_df
        ru_mean = data_ru_df['rank'].mean()
        ru_median = data_ru_df['rank'].quantile(0.5)
        return {'ru_mean': ru_mean, 'ru_median': ru_median}

    @task()
    def filter_data_com(data) -> pd.Series:
        data_df = pd.read_csv(StringIO(data))
        data_com = (
            data_df
            [data_df['domain'].str.endswith('.com')]
        )
        return data_com.to_csv(index=False)

    @task()
    def calc_stat_com(data_com_df) -> dict:
        data_com_df = data_com_df
        com_mean = data_com_df['rank'].mean()
        com_median = data_com_df['rank'].quantile(0.5)
        return {'com_mean': com_mean, 'com_median': com_median}

    @task()
    def print_data(stat_ru, stat_com) -> print:
        context = get_current_context()
        date = context['ds']

        ru_mean, ru_median = stat_ru['ru_mean'], stat_ru['ru_median']
        com_mean, com_median = stat_com['com_mean'], stat_com['com_median']

        print(f"\nRank stat in .ru for date {date}:")
        print(f"Mean - {ru_mean}")
        print(f"Median - {ru_median}")

        print(f"\nRank stat in .com for date {date}:")
        print(f"Mean - {com_mean}")
        print(f"Median - {com_median}")

    # DAG algorithm
    data = get_data()
    data_ru = filter_data_ru(data)
    data_com = filter_data_com(data)
    stat_ru = calc_stat_ru(data_ru)
    stat_com = calc_stat_com(data_com)
    print_data(stat_ru, stat_com)

# DAG start
top_10_airflow_pg = top_10_airflow_pg()
