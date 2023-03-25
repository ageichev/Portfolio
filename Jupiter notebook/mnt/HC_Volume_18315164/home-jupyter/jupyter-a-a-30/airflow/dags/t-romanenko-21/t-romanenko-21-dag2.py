import requests
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

default_args = {
    'owner': 't-romanenko-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4),
    'schedule_interval': '00 10 * * *'
}

CHAT_ID = 972306061
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 't-romanenko-21'
get_year = 1994 + hash(f'{login}') % 23


@dag(default_args=default_args, catchup=False)
def romanenko_dag_lesson3_answers():
    @task()
    def get_data():
        games_df = pd.read_csv(path)
        games_df = games_df.query('Year == @get_year')
        return games_df

    @task()
    def get_bestseller_game(games_df):
        bestseller_game = games_df.groupby('Name') \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .head(1).index[0]
        return bestseller_game

    @task()
    def get_EU_bestseller_genre(games_df):
        EU_bestseller_genre = games_df.groupby('Genre') \
            .agg({'EU_Sales': 'sum'}) \
            .query('EU_Sales == EU_Sales.max()').reset_index().Genre[0]
        return EU_bestseller_genre

    @task()
    def get_popular_platform_NA(games_df):
        popular_platform_NA = games_df.query('NA_Sales > 1').groupby('Platform') \
            .agg({'Name': 'count'}) \
            .query('Name == Name.max()').reset_index().Platform[0]
        return popular_platform_NA

    @task()
    def get_mean_sales_JP_publisher(games_df):
        mean_sales_JP_publisher = games_df.groupby('Publisher') \
            .agg({'JP_Sales': 'mean'}) \
            .query('JP_Sales == JP_Sales.max()').reset_index().Publisher[0]
        return mean_sales_JP_publisher

    @task()
    def get_EU_vs_JP(games_df):
        EU_vs_JP = games_df.groupby('Name', as_index=False) \
            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
            .query('EU_Sales > JP_Sales') \
            .Name.count()
        return EU_vs_JP

    @task(on_success_callback=send_message)
    def print_data(bestseller_game, EU_bestseller_genre, \
                   popular_platform_NA, mean_sales_JP_publisher, \
                   EU_vs_JP):

        context = get_current_context()
        date = context['ds']

        print(f'''Today is {date}''')

        print(f'''Global bestseller for {get_year} is {bestseller_game}''')

        print(f'''European bestseller for {get_year} is {EU_bestseller_genre}''')

        print(f'''Most popular platforn in NA for {get_year} is {popular_platform_NA}''')

        print(f'''JP publisher with highest mean sales for {get_year} is {mean_sales_JP_publisher}''')

        print(f'''Quantity of games sold better in EU than JP for {get_year} is {EU_vs_JP}''')

    games_df = get_data()
    bestseller_game = get_bestseller_game(games_df)
    EU_bestseller_genre = get_EU_bestseller_genre(games_df)
    popular_platform_NA = get_popular_platform_NA(games_df)
    mean_sales_JP_publisher = get_mean_sales_JP_publisher(games_df)
    EU_vs_JP = get_EU_vs_JP(games_df)
    print_data = print_data(bestseller_game, EU_bestseller_genre, popular_platform_NA, \
                            mean_sales_JP_publisher, EU_vs_JP)

romanenko_dag_lesson3_answers = romanenko_dag_lesson3_answers()
