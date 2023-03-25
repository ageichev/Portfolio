import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'n-bagro-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 13)
   }

CHAT_ID = 5083826152
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = '5218702532:AAG1ecvm7IhivTi130TAyMJQ24LHhtcQDvw'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass
    
@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def lesson_3_airflow_n_bagro_20():
    @task(retries=3)
    def get_data():
        my_year = 1994 + hash(f'n-bagro-20') % 23
        data = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        df = data.query('Year == @my_year')
        return df
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_top_game(df):
        df_1 = df.groupby('Name', as_index=False)\
                 .agg({'Global_Sales':'sum'})\
                 .sort_values('Global_Sales', ascending=False)
        top_game = df_1.head(1).Name.to_list()
        return top_game
    
    @task()
    def get_top_genre(df):
        df_2 = df.groupby('Genre', as_index=False)\
                 .agg({'EU_Sales':'sum'})\
                 .sort_values('EU_Sales', ascending=False)
        max_2 = df_2.EU_Sales.max()
        top_genre = df_2.query('EU_Sales == @max_2').Genre.to_list()
        return top_genre
    
    @task()
    def get_top_platform(df):
        df_3 = df.groupby(['Platform', 'Name'], as_index=False)\
                 .agg({'NA_Sales':'sum'})\
                 .query('NA_Sales > 1')\
                 .sort_values('NA_Sales', ascending=False)
        df_3_final = df_3.groupby('Platform', as_index=False)\
                         .agg({'Name':'count'})\
                         .rename(columns={'Name':'Quantity'})
        max_3 = df_3_final.Quantity.max()
        top_platform = df_3_final.query('Quantity == @max_3').Platform.to_list()
        return top_platform
    
    @task()
    def get_top_publisher(df):
        df_4 = df.groupby('Publisher', as_index=False)\
                 .agg({'JP_Sales':'mean'})\
                 .sort_values('JP_Sales', ascending=False)
        max_4 = df_4.JP_Sales.max()
        top_publisher = df_4.query('JP_Sales == @max_4').Publisher.to_list()
        return top_publisher
    
    @task()
    def get_top_EU_games(df):
        df_5 = df.groupby('Name', as_index=False)\
                 .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})\
                 .query('EU_Sales > JP_Sales')
        top_EU_games = df_5.shape[0]
        return {'top_EU_games': top_EU_games}
    
    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre, top_platform, top_publisher, EU_games):

        my_year = 1994 + hash(f'n-bagro-20') % 23

        top_EU_games = EU_games['top_EU_games']

        print(f'''1.Самой продаваемой во всем мире в {my_year} году была игра: {top_game}''')
        print(f'''2.Игры следующих жанров были самыми продаваемыми в Европе в {my_year} году: {top_genre}''') 
        print(f'''3.На следующих платформах было больше всего игр, которые продались
                           более чем миллионным тиражом в Северной Америке в {my_year} году: {top_platform}''')
        print(f'''4.У следующего издателя самые высокие средние продажи в Японии в {my_year} году: {top_publisher}''')
        print(f'''5.Количество игр, которые продались лучше в Европе, чем в Японии в {my_year} году: {top_EU_games}''')
        
    df = get_data()
    top_game = get_top_game(df)
    top_genre = get_top_genre(df)
    top_platform = get_top_platform(df)
    top_publisher = get_top_publisher(df)
    games_data = get_top_EU_games(df)
    print_data(top_game, top_genre, top_platform, top_publisher, games_data)
    
lesson_3_airflow_n_bagro_20 = lesson_3_airflow_n_bagro_20()