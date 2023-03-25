import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'v-dvoeglazov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
    'schedule_interval': '0 10 * * *'
}

year = 1994 + hash(f'v-dvoeglazov-23') % 23

@dag(default_args=default_args, catchup=False)
def v_dvoeglazov_23_lesson_03():
    @task()
    def get_data():
        df = pd.read_csv(vgsales)
        return df

    @task()
    def get_top_name(df):
        return df.query("Year == @year").groupby('Name').sum().Global_Sales.idxmax()

    @task()
    def get_eu_genre(df):
        return df.query("Year == @year").groupby('Genre').sum().EU_Sales.idxmax()

    @task()
    def get_na_platform(df):
        return df.query("Year == @year & NA_Sales > 1").groupby('Platform').sum().NA_Sales.idxmax()

    @task()
    def get_jp_publisher(df):
        return df.query("Year == @year").groupby('Publisher').mean().JP_Sales.idxmax()

    @task()
    def get_games_eu_vs_jp(df):
        return (df.query("Year == @year").EU_Sales > df.query("Year == @year").JP_Sales).sum()

    @task()
    def print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp):
        print(f'''
            Top sales game worldwide in {year}: {top_game}
            Top genre in EU in {year}: {eu_genre}
            Top platform in North America in {year}: {na_platform}
            Top publisher in Japan in {year}: {jp_publisher}
            Number of Games EU vs. JP in {year}: {games_eu_vs_jp}''')

    df = get_data()

    top_game = get_top_name(df)
    eu_genre = get_eu_genre(df)
    na_platform = get_na_platform(df)
    jp_publisher = get_jp_publisher(df)
    games_eu_vs_jp = get_games_eu_vs_jp(df)

    print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp)

v_dvoeglazov_23_lesson_03 = v_dvoeglazov_23_lesson_03()