import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import  StringIO
import telegram

from airflow.decorators import dag,task



data_file='/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'g-antonova-23'
year = 1994+hash(f'{login}')%23

default_args = {
    'owner' : 'g-antonova-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 22),
    'schedule_interval' : '0 18 * * *'
}

CHAT_ID = -614629108
bot_token = '5136116305:AAEatjowv3KoTlX9pleglsaez18Mhv605Xg'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Success! Dag {dag_id} completed on {date}'
    bot=telegram.Bot(token=bot_token)
    bot.send_message(chat_id = CHAT_ID, text = message)

@dag(default_args=default_args, catchup=False)
def g_antonova_23_lesson3():
    @task()
    def get_data():
        games_sales = pd.read_csv(data_file)
        games_sales_year = games_sales.query('Year==@year')
        return games_sales_year

    @task()
    def get_most_popular_game(games_sales_year):
        most_popular_game = games_sales_year.loc[games_sales_year.Global_Sales.idxmax()].Name
        return most_popular_game

    @task()
    def get_eu_popular_genre(games_sales_year):
        eu_popular_genre = games_sales_year.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .query('EU_Sales==EU_Sales.max()')
        return eu_popular_genre

    @task()
    def get_platform_with_popular_game_NA(games_sales_year):
        platform_with_popular_game_NA = games_sales_year.query('NA_Sales>=1') \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'count'}) \
            .query('NA_Sales==NA_Sales.max()').Platform
        return platform_with_popular_game_NA

    @task
    def get_publisher_with_max_sales_jp(games_sales_year):
        publisher_with_max_sales_jp = games_sales_year.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .query('JP_Sales==JP_Sales.max()').Publisher
        return publisher_with_max_sales_jp

    @task
    def get_games_value_with_better_eu_sales_than_jp(games_sales_year):
        games_value_with_better_eu_sales_than_jp = games_sales_year.query('EU_Sales>JP_Sales').shape[0]
        return games_value_with_better_eu_sales_than_jp

    @task(on_success_callback=send_message)
    def print_data(most_popular_game,
                   eu_popular_genre,
                   get_platform_with_popular_game_NA,
                   get_publisher_with_max_sales_jp,
                   games_value_with_better_eu_sales_than_jp):

        print(f'Most popular game in {year}')
        print(most_popular_game)

        print(f'Most popular genre in Europe in {year}')
        print(eu_popular_genre)

        print(f'Platform with more than 1 million sales sgames in North America in {year}')
        print(get_platform_with_popular_game_NA)

        print(f'Publisher with max sales Japan in {year}')
        print(get_publisher_with_max_sales_jp)

        print(f'Games value with sales better in Europe than in Japan in {year}')
        print(games_value_with_better_eu_sales_than_jp)

    games_sales_year = get_data()
    most_popular_game = get_most_popular_game(games_sales_year)
    eu_popular_genre = get_eu_popular_genre(games_sales_year)
    platform_with_popular_game_NA = get_platform_with_popular_game_NA(games_sales_year)
    publisher_with_max_sales_jp = get_publisher_with_max_sales_jp(games_sales_year)
    games_value_with_better_eu_sales_than_jp = get_games_value_with_better_eu_sales_than_jp(games_sales_year)
    print_data(most_popular_game,
               eu_popular_genre,
               platform_with_popular_game_NA,
               publisher_with_max_sales_jp,
               games_value_with_better_eu_sales_than_jp)


g_antonova_23_lesson3 = g_antonova_23_lesson3()
