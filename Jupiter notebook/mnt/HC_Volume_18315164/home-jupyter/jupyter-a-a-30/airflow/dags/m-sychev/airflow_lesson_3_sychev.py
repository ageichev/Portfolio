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

df_game_saels = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
my_year = 1994 + hash('m-sychev') % 23

default_args = {
    'owner': 'm.sychev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 17),
    'schedule_interval': '30 10 * * *'
}

@dag(default_args=default_args)
def sychev_airflow_lesson_3():
    @task()
    def get_data(df_game_saels):
        game_saels = pd.read_csv(df_game_saels)
        game_saels = game_saels.loc[game_saels.Year == my_year]
        return game_saels

    @task()
    def most_popular_game(game_saels):
        top_game = game_saels \
                        .groupby('Name', as_index=False) \
                        .agg({'Global_Sales' : 'sum'}) \
                        .sort_values('Global_Sales', ascending=False) \
                        .head(1)['Name'] \
                        .values[0]
        return top_game

    @task()
    def top_genre(game_saels):
        genre_sales = game_saels \
                            .groupby('Genre', as_index=False) \
                            .agg({'EU_Sales' : 'sum'}) \
                            .sort_values('EU_Sales', ascending=False)
        top_genre_list = list(genre_sales.loc[genre_sales.EU_Sales == genre_sales.EU_Sales.values[0]]['Genre'])
        return top_genre_list

    @task()
    def top_platform(game_saels):
        top_platform  = game_saels \
                            .loc[game_saels.NA_Sales > 1] \
                            .groupby('Platform', as_index=False) \
                            .agg({'Name' : 'count'}) \
                            .sort_values('Name', ascending=False)
        top_platform_list = list(top_platform.loc[top_platform.Name == top_platform.Name.values[0]]['Platform'])
        return top_platform_list

    @task()
    def top_publisher_japan(game_saels):
        top_japan = game_saels \
                .groupby('Publisher', as_index=False) \
                .agg({'JP_Sales' : 'mean'}) \
                .sort_values('JP_Sales', ascending=False)
        top_japan_list = list(top_japan.loc[top_japan.JP_Sales == top_japan.JP_Sales.values[0]]['Publisher'])
        return top_japan_list

    @task()
    def sales_eu_better_jp(game_saels):
        eu_better_jp = game_saels \
                    .groupby('Name', as_index=False) \
                    .agg({"EU_Sales":"sum", "JP_Sales":"sum"}) \
                    .query('EU_Sales > JP_Sales') \
                    .shape[0]
        return eu_better_jp 
    
    @task()
    def print_data(top_game, top_genre_list, top_platform_list, top_japan_list, eu_better_jp):
        print(f'The best sales game in {my_year}: {top_game}')
        print(f'Top ganres in Europe in {my_year}: {top_genre_list}')
        print(f'Top platforms (> 1 million game copy) in {my_year} in North America: {top_platform_list}')
        print(f'Top publisher in Japan in {my_year}: {top_japan_list}')
        print(f'More popular in Europe than in Japan in {my_year}: {eu_better_jp}')

    game_saels = get_data(df_game_saels)
    top_game = most_popular_game(game_saels)
    top_genre_list = top_genre(game_saels)
    top_platform_list = top_platform(game_saels)
    top_japan_list = top_publisher_japan(game_saels)
    eu_better_jp = sales_eu_better_jp(game_saels)
    print_data(top_game, top_genre_list, top_platform_list, top_japan_list, eu_better_jp)

sychev_airflow_lesson_3 = sychev_airflow_lesson_3()
