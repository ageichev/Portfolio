import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'b.argymbek-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 06, 30),
    'schedule_interval': '0 10 * * *'
}

y = 1994 + hash(f'b.argymbek-18') % 23


@dag(default_args=default_args, catchup=False)
def b_argymbek_18_dag_lesson3():
    @task()
    def get_data():
        df = pd.read_csv(vgsales)
        return df

    @task()
    def get_top_name(df):
        return df.query("Year == @y").groupby('Name').sum().Global_Sales.idxmax()

    @task()
    def get_eu_genre(df):
        return df.query("Year == @y").groupby('Genre').sum().EU_Sales.idxmax()

    @task()
    def get_na_platform(df):
        return df.query("Year == @y & NA_Sales > 1").groupby('Platform').sum().NA_Sales.idxmax()

    @task()
    def get_jp_publisher(df):
        return df.query("Year == @y").groupby('Publisher').mean().JP_Sales.idxmax()

    @task()
    def get_games_eu_vs_jp(df):
        return (df.query("Year == @y").EU_Sales > df.query("Year == @y").JP_Sales).sum()

    @task()
    def print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp):
        print(f'''
            Top sales game worldwide in {y}: {top_game}
            Top genre in EU in {y}: {eu_genre}
            Top platform in North America in {y}: {na_platform}
            Top publisher in Japan in {y}: {jp_publisher}
            Number of Games EU vs. JP in {y}: {games_eu_vs_jp}''')

    df = get_data()

    top_game = get_top_name(df)
    eu_genre = get_eu_genre(df)
    na_platform = get_na_platform(df)
    jp_publisher = get_jp_publisher(df)
    games_eu_vs_jp = get_games_eu_vs_jp(df)

    print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp)


b_argymbek_18_dag_lesson3 = b_argymbek_18_dag_lesson3()
