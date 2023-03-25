import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-pivovarov-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 3),
    'schedule_interval': '0 20 * * *'
}

y = 1994 + hash(f'a-pivovarov-25') % 23

@dag(default_args=default_args, catchup=False)


def a_pivovarov_hw_3():
    @task()
    def get_data():
        data = pd.read_csv(vgsales)
        return data

    @task()
    def get_top_name(data):
        global_top = data.groupby('Name', as_index=False) \
                        .Global_Sales.sum() \
                        .sort_values('Global_Sales', ascending=False) \
                        .iloc[0,0]
        return global_top
    
    @task()
    def get_eu_genre(data):
        top_eu_genre = data.groupby('Genre', as_index=False).EU_Sales.sum().sort_values('EU_Sales', ascending=False)
        top_eu_genre = top_eu_genre[top_eu_genre.EU_Sales == top_eu_genre.EU_Sales.max()].Genre.to_list()
        return top_eu_genre

    @task()
    def get_na_platform(data):
        top_platform_NA = data.loc[data.NA_Sales > 1] \
                                    .groupby('Platform', as_index=False) \
                                    .agg({'Name': 'count'}) \
                                    .sort_values('Name', ascending=False) \
                                    .rename(columns={'Name': 'Number'})
        top_platform_NA = top_platform_NA[top_platform_NA.Number == top_platform_NA.Number.max()].Platform.to_list()
        return top_platform_NA

    @task()
    def get_jp_publisher(data):
        top_publisher_JP = data.groupby('Publisher', as_index=False).JP_Sales.mean().sort_values('JP_Sales', ascending=False)
        top_publisher_JP = top_publisher_JP[top_publisher_JP.JP_Sales == top_publisher_JP.JP_Sales.max()].Publisher.to_list()
        return top_publisher_JP

    @task()
    def get_games_eu_vs_jp(data):
        EU_beat_JP =  data.query('EU_Sales > JP_Sales').shape[0]
        return EU_beat_JP
    
    @task()
    def print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp):
        print(f'''
            Top sales game worldwide in {y}: {top_game}
            Top genre in EU in {y}: {eu_genre}
            Top platform in North America in {y}: {na_platform}
            Top publisher in Japan in {y}: {jp_publisher}
            Number of Games EU vs. JP in {y}: {games_eu_vs_jp}''')

    data = get_data()

    top_game = get_top_name(data)
    eu_genre = get_eu_genre(data)
    na_platform = get_na_platform(data)
    jp_publisher = get_jp_publisher(data)
    games_eu_vs_jp = get_games_eu_vs_jp(data)

    print_data(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp)


a_pivovarov_hw_3 = a_pivovarov_hw_3()