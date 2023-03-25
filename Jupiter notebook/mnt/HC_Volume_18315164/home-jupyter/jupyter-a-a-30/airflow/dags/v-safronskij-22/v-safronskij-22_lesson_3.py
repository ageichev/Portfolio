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


default_args = {
    'owner': 'v-safronskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
    'schedule_interval': '0 6 * * *'
}

my_year = 1994 + hash('v-safronskij') % 23


CHAT_ID = -643518002
try:
    BOT_TOKEN = '5271567829:AAFTgcKfoZ_oCPHvSY4hlszWgnPB4xZqXRk'
except:
    BOT_TOKEN = ''


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Oh my! Dag {dag_id} with data for {my_year} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


@dag(default_args=default_args, catchup=False)
def v_safronskij_lesson3_dag():
    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df['Year'] = df.Year.fillna('0').astype(int)
        df_my_year = df.query('Year == @my_year')
        return df_my_year

    @task()
    def get_top_game(df_my_year):
        top_game = df_my_year.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales').tail(1)['Name'].to_list()
        top_game = ', '.join(top_game)
        return top_game

    @task()
    def get_top_genre_europe(df_my_year):
        top_sales_europe = df_my_year.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})['EU_Sales'].max()
        top_genre_europe = df_my_year.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).query('EU_Sales == @top_sales_europe')['Genre'].to_list()
        top_genre_europe = ', '.join(top_genre_europe)
        return top_genre_europe


    @task()
    def get_top_NA_platform(df_my_year):
        top_NA_platform_games = df_my_year.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'Name': 'count'}).Name.max()
        top_NA_platform = df_my_year.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'Name': 'count'}).query('Name == @top_NA_platform_games')['Platform'].to_list()
        top_NA_platform = ', '.join(top_NA_platform)
        return top_NA_platform


    @task()
    def get_top_avg_sales_Japan(df_my_year):
        top_JP_avg_sales = df_my_year.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).JP_Sales.max()
        top_avg_sales_Japan = df_my_year.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).query('JP_Sales == @top_JP_avg_sales')['Publisher'].to_list()
        top_avg_sales_Japan = ', '.join(top_avg_sales_Japan)
        return top_avg_sales_Japan

    @task()
    def get_Europe_than_Japan(df_my_year):
        Europe_than_Japan = df_my_year.query('EU_Sales > JP_Sales').shape[0]
        return Europe_than_Japan

    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre_europe, top_NA_platform, top_avg_sales_Japan, Europe_than_Japan):
        print(f'In {my_year} the worldwide best-selling game was {top_game}')
        print(f'In {my_year} the best-selling genre in Europe was {top_genre_europe}')
        print(f'In {my_year} in North America, {top_NA_platform} platform had the best figures in sold games')
        print(f'In {my_year} , {top_avg_sales_Japan} publisher had the highest average sales in Japan')
        print(f'In {my_year} Europe sold {Europe_than_Japan}  more game copies than Japan ')

    df_my_year = get_data()

    top_game = get_top_game(df_my_year)
    top_genre_europe = get_top_genre_europe(df_my_year)
    top_NA_platform = get_top_NA_platform(df_my_year)
    top_avg_sales_Japan = get_top_avg_sales_Japan(df_my_year)
    Europe_than_Japan = get_Europe_than_Japan(df_my_year)

    print_data(top_game, top_genre_europe, top_NA_platform, top_avg_sales_Japan, Europe_than_Japan)


v_safronskij_lesson3_dag = v_safronskij_lesson3_dag()

