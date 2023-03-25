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


vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'd-pentjuhov'
year  = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'd.pentjuhov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 27),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)

def d_pentjuhov_hw3():
    
    @task()
    def get_data():
        df = pd.read_csv(vgsales)
        return df

    @task()
    def get_top_game(df):
        return df.query('Year == @year').groupby('Name').agg(np.sum).Global_Sales.idxmax()

    @task()
    def get_eu_genre(df):
        return df.query('Year == @year').groupby('Genre').agg({'EU_Sales': 'sum'}).EU_Sales.idxmax()

    @task()
    def get_na_platform(df):
        return df.query('Year == @year & NA_Sales > 1').groupby('Platform').agg({'NA_Sales': 'sum'}).NA_Sales.idxmax()

    @task()
    def get_jp_publisher(df):
        return df.query('Year == @year').groupby('Publisher').agg({'JP_Sales': 'mean'}).JP_Sales.idxmax()

    @task()
    def get_eu_vs_jp_games(df):
        return (df.query("Year == @year").EU_Sales > df.query("Year == @year").JP_Sales).sum()

    @task()
    def print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp):
        print(f'''
            Top sales game worldwide in {year}: {top_game}
            Top genre in EU in {year}: {eu_genre}
            Top platform in North America in {year}: {na_platform}
            Top publisher in Japan in {year}: {jp_publisher}
            Number of games EU vs JP in {year}: {games_eu_vs_jp}''')

    df = get_data()
    top_game = get_top_game(df)
    eu_genre = get_eu_genre(df)
    na_platform = get_na_platform(df)
    jp_publisher = get_jp_publisher(df)
    games_eu_vs_jp = get_eu_vs_jp_games(df)

    print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp)

    
d_pentjuhov_hw3 = d_pentjuhov_hw3()
