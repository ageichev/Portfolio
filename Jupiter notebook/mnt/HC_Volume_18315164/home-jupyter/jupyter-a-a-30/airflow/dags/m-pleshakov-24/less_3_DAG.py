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

PATH_SALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'm.pleshakov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 20),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 278803016
BOT_TOKEN = '5566102599:AAEo1veb64fXRzQfndClmHC_8A0oM9BM2GY'
YEAR = 1994 + hash(f'm-pleshakov-24') % 23


@dag(default_args=default_args, catchup=False)
def task_less3_airflow_2():
    
    @task(retries=3)
    def get_data():
        df = pd.read_csv(PATH_SALES)
        return df.to_csv(index=False)

    @task() # 1 Какая игра была самой продаваемой в этом году во всем мире?
    def get_most_sales(df):
        df = pd.read_csv(StringIO(df))
        most_sales = df[(df.Year == YEAR) & (df.Global_Sales == df[df.Year == YEAR].Global_Sales.max())].Name.values
        most_sales = str(*most_sales)
        return most_sales

    @task() # 2 Игры какого жанра были самыми продаваемыми в Европе?
    def get_Genre(df):
        df = pd.read_csv(StringIO(df))
        Genre = df.groupby('Genre', as_index=False).EU_Sales.sum()
        Genre = Genre[Genre.EU_Sales == Genre.EU_Sales.max()].Genre.values
        Genre = str(*Genre)
        return Genre
    
    # 3 На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task() 
    def get_Platform(df):
        df = pd.read_csv(StringIO(df))
        Platform = df[df.NA_Sales > 1].groupby('Platform',as_index=False) \
                    .agg({'Name':'nunique'}) \
                    .sort_values('Name')
        Platform = str(*Platform[Platform.Name == Platform.Name.max()].Platform.values)
        return Platform
    
    @task() #4 У какого издателя самые высокие средние продажи в Японии?
    def get_Publisher(df):
        df = pd.read_csv(StringIO(df))
        Publisher = df.groupby('Publisher',as_index=False) \
        .agg({'JP_Sales':'sum'}).sort_values('JP_Sales')[::-1]
        Publisher = str(*Publisher[Publisher.JP_Sales == Publisher.JP_Sales.max()].Publisher.values)
        return Publisher

    @task() #5 Сколько игр продались лучше в Европе, чем в Японии?
    def get_games(df):
        df = pd.read_csv(StringIO(df))
        games = df.groupby('Name',as_index=False) \
                    .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        games = str(games[games.EU_Sales > games.JP_Sales].Name.count())
        return games

    @task()
    def print_data(most_sales, Genre, Publisher, Platform, games):

        context = get_current_context()
        date = context['ds']

        final = f'''Report {date}:
                  1. Игра {most_sales} была самой продаваемой в {YEAR} году во всем мире.
                  2. Игры {Genre} жанра были самыми продаваемыми в Европе.
                  3. На платформе {Platform} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке.
                  4. У издателя {Publisher} самые высокие средние продажи в Японии.
                  5. {games} игр продались лучше в Европе, чем в Японии.
                  '''
        return final

    @task()
    def send_message(final):
        context = get_current_context()
        date = context['ds']
        dag_id = context['dag'].dag_id
        message = final
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
     
        
    df = get_data()
    most_sales = get_most_sales(df)
    Genre = get_Genre(df)
    Publisher = get_Publisher(df)
    Platform = get_Platform(df)
    games = get_games(df)
    
    final = print_data(most_sales, Genre, Publisher, Platform, games)
    send_message(final)
    

task_less3_airflow_2 = task_less3_airflow_2()
