import requests
#from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
#from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

#url = 'https://git.lab.karpov.courses/lab/airflow/-/blob/master/dags/a.batalov/vgsales.csv'

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
interested_year = 1994 + hash(f'o-chemjakina-25') % 23

default_args = {
    'owner': 'o-chemjakina-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 30),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 510043289
BOT_TOKEN = '5504912087:AAF0_pyNWlQ-RUStXP0Um3NGSOEaH4KSNIs'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'DAG {dag_id} was completed successfully at {date}!'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False)
def o_chemjakina_games_sales_airflow_2():

    @task()
    def get_data(path, interested_year):
        games_data = pd.read_csv(path)
        games_data = games_data.loc[games_data.Year == interested_year]
        return games_data

    @task()
    def get_bestseller_game(games_data):
        bestseller_game = games_data.groupby('Name', as_index=False) \
                                    .agg({'Global_Sales':'sum'}) \
                                    .sort_values('Global_Sales', ascending = False) \
                                    .head(1) \
                                    .Name.item()
        return bestseller_game

    @task()
    def get_bestseller_genre_eu(games_data):
        bestseller_genre_eu = games_data.groupby('Genre', as_index=False) \
                                        .agg({'EU_Sales':'sum'}) \
                                        .sort_values('EU_Sales', ascending = False) \
                                        .head(1) \
                                        .Genre.item()
        return bestseller_genre_eu

    @task()
    def get_bestseller_platform_na_1m(games_data):
        bestseller_platform_na_1m = games_data.loc[games_data.NA_Sales > 1] \
                                            .groupby('Platform', as_index=False) \
                                            .agg({'Name':'count'}) \
                                            .sort_values('Name', ascending = False) \
                                            .head(1) \
                                            .Platform.item()
        return bestseller_platform_na_1m

    
    @task()
    def get_highest_sales_publisher_jp(games_data):
        highest_sales_publisher_jp = games_data.groupby('Publisher', as_index = False) \
                                                .agg({'JP_Sales':'mean'}) \
                                                .sort_values('JP_Sales', ascending = False) \
                                                .head(1) \
                                                .Publisher.item()
        return highest_sales_publisher_jp
    
    @task()
    def get_num_games_sold_better_in_eu_than_jp(games_data):
        num_games_sold_better_in_eu_than_jp = games_data.query('JP_Sales < EU_Sales') \
                                                        .value_counts('Name') \
                                                        .head(1) \
                                                        .item()
        return num_games_sold_better_in_eu_than_jp
    
    @task(on_success_callback=send_message)
    def print_data(bestseller_game,
                   bestseller_genre_eu,
                   bestseller_platform_na_1m,
                   highest_sales_publisher_jp,
                   num_games_sold_better_in_eu_than_jp):
        context = get_current_context()
        date = context['ds']
        print(f'Bestseller game of {interested_year}: {bestseller_game}')
        print(f'Bestseller genre of {interested_year}: {bestseller_genre_eu}')
        print(f'Bestseller platform on that games was sold over 1M copies in NA in {interested_year}: {bestseller_platform_na_1m}')
        print(f'Publisher with the highest average sales in Japan in {interested_year}: {highest_sales_publisher_jp}')
        print(f'How many games was sold better in Europe than in Japan in {interested_year}: {num_games_sold_better_in_eu_than_jp}')

    games_data = get_data(path, interested_year)
    bestseller_game = get_bestseller_game(games_data)
    bestseller_genre_eu = get_bestseller_genre_eu(games_data)
    bestseller_platform_na_1m = get_bestseller_platform_na_1m(games_data)
    highest_sales_publisher_jp = get_highest_sales_publisher_jp(games_data)
    num_games_sold_better_in_eu_than_jp = get_num_games_sold_better_in_eu_than_jp(games_data)
    print_data(bestseller_game, 
               bestseller_genre_eu, 
               bestseller_platform_na_1m, 
               highest_sales_publisher_jp, 
               num_games_sold_better_in_eu_than_jp)

o_chemjakina_games_sales_airflow_2 = o_chemjakina_games_sales_airflow_2()