import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'k-zemskij-26'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'k.zemskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 18),
    'schedule_interval': '0 1 * * *'
}


@dag(default_args=default_args, catchup=False)
def k_zemskij_26_lesson3():
    @task(retries=2)
    def get_data():
        games = pd.read_csv(vgsales)
        games = games.query('Year == @year')
        return games

    @task()
    def best_seller_game(games):
#       group by name because in one year games can be sold in different platform so we need to sum it
        best_seller = games.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'})
#       filter data by max value
        best_seller = best_seller[best_seller.Global_Sales == best_seller.Global_Sales.max()].Name.to_list()
        return best_seller

    @task()
    def eu_best_genre(games):
#       find the best-selling genre in worldwide
        best_genre_eu = games.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        best_genre_eu = best_genre_eu[best_genre_eu.EU_Sales == best_genre_eu.EU_Sales.max()].Genre.to_list()
        return best_genre_eu

    @task()
    def best_platform_for_game(games):
#       find platform had the most games that sold over a million copies in NA
        best_platform = games.query('NA_Sales > 1').groupby('Platform', as_index=False).agg(quantity=('Name', 'count'))
        best_platform = best_platform[best_platform.quantity == best_platform.quantity.max()].Platform.to_list()
        return best_platform
    
    @task()
    def best_japan_publisher(games):
#       find publisher has the highest average sales in Japan
        best_japan = games.groupby('Publisher', as_index=False).agg(mean_sales=('JP_Sales', 'mean'))
        best_japan = best_japan[best_japan.mean_sales == best_japan.mean_sales.max()].Publisher.to_list()
        return best_japan

    @task()
    def eu_versus_jp(games):
#       find the number of games that sold better in Europe than in Japan
        eu_vs_jp = games.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).query('EU_Sales > JP_Sales').shape[0]
        return eu_vs_jp

    @task()
    def print_data(best_seller, best_genre_eu, best_platform, best_japan, eu_vs_jp):

        context = get_current_context()
        date = context['ds']

        print(f'''Data from games in world for {year} year for {date}
                  Game was the best-selling worldwide: {best_seller}
                  Genre of games were the best-selling in Europe: {best_genre_eu}
                  Platform had the most games that sold over a million copies in NA: {best_platform}
                  Publisher has the highest average sales in Japan: {best_japan}
                  Games have sold better in Europe than in Japan: {eu_vs_jp}''')

    games = get_data()
    best_seller = best_seller_game(games)
    best_genre_eu = eu_best_genre(games)
    best_platform = best_platform_for_game(games)
    best_japan = best_japan_publisher(games)
    eu_vs_jp = eu_versus_jp(games)

    print_data(best_seller, best_genre_eu, best_platform, best_japan, eu_vs_jp)

k_zemskij_26_lesson3 = k_zemskij_26_lesson3()