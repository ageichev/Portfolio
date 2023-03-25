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
    'owner': 'd-odintsov-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 19),
    'schedule_interval': '0 14 * * *'
}
# бот находится по @airflow_test_karpov_course_bot
# нужно будет узнать свой CHAT_ID по ссылке https://api.telegram.org/bot<Bot_token>/getUpdates
CHAT_ID = 395485317 
try:
    BOT_TOKEN = '5644790190:AAEw-ILz5_xK-p6cDMmWBKl33wvO3J3Ha4A'
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

@dag(default_args=default_args, catchup=False)
def games_stat_24_od():
    @task(retries=3)
    def get_table():
        my_year = 1994 + hash('f‘{d-odintsov-24}') % 23
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv', sep=',')
        df['Year'] = df['Year'].astype('Int64')
        df_1 = df.query('Year == @my_year')
        return(df_1)

    @task(retries=4, retry_delay=timedelta(10))
    def top_game(df_1):
        top_game_1 = df_1.query('Global_Sales == @df_1.Global_Sales.max()').Name
        return(top_game_1)

    @task()
    def top_genre_eu_sales(df_1):    
        top_genre_eu = df_1.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})\
                           .rename(columns={'EU_Sales': 'SUM_EU_Sales'})
        top = top_genre_eu.query('SUM_EU_Sales == @top_genre_eu.SUM_EU_Sales.max()')
        return(top.to_csv(index=False))
    @task()
    def top_platform_NA(df_1):
        top_NA = df_1.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'Name': 'count'})\
                     .rename(columns={'Name': 'number_of_games'})
        top =top_NA.query('number_of_games == @top_NA.number_of_games.max()')
        return(top.to_csv(index=False))

    @task()
    def top_publisher_JP(df_1):
        top_publisher = df_1.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})\
                            .rename(columns={'JP_Sales': 'mean_JP_sales'})
        top = top_publisher.query('mean_JP_sales == @top_publisher.mean_JP_sales.max()')
        return(top)
    
    @task()
    def EU_better_JPs(df_1):
        return(df_1.query('JP_Sales < EU_Sales').Name.nunique())

    @task(on_success_callback=send_message)
    def print_data(top, top_genre, top_platform, top_publisher, EU_better):

        print('Best Game\n')
        print(top, '\n')

        print('Top selling genre in Europe\n')
        print(top_genre, '\n')
        
        print('Top platform with millions of sales in North America\n')
        print(top_platform, '\n')
        
        print('Publisher with the highest average sales in Japan\n')
        print(top_publisher, '\n')
        
        print('Number of games with big sales in Europe compared to Japan\n')
        print(EU_better, '\n')

    df = get_table()
    top = top_game(df)
    top_genre = top_genre_eu_sales(df)
    top_platform = top_platform_NA(df)
    top_publisher = top_publisher_JP(df)
    EU_better = EU_better_JPs(df)
    print_data(top, top_genre, top_platform, top_publisher, EU_better)

games_stat_24_od = games_stat_24_od()