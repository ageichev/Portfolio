import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable




import json
from urllib.parse import urlencode


default_args = {
    'owner': 'a.tomilin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 20),
    'schedule_interval': '30 * * * *'
}

name = 'Megabot'
user_name = 'Bot_MegaHobot'
BOT_TOKEN = '5124556530:AAFCYst3qIeQ5FpWuCccxYavNZDX54EAnkU'
CHAT_ID = 1143999071  # your chat id


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


@dag(default_args=default_args, catchup = False)

def a_tomilin_game_stat():

    @task()
    def get_data():
        '''
        Считывает данные из файла и оставляет только тот год, который соответствует заданию

        Запасной путь:
            path = 'var/lib/airflow.git/dags/a.batalov/vgsales.csv'
            data = pd.read_csv(path)

        top_games = pd.read_csv('vgsales.csv')
        
        https://docs.google.com/spreadsheets/d/1dc5UmXm-CJUHR23H9Ljhcfg0PlFMbAg8/edit?usp=drivesdk&ouid=117628431647611713368&rtpof=true&sd=true
        
        link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'
        path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]
        top_games = pd.read_csv(path)
        
        '''
        link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'

        path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]

        top_games = pd.read_csv(path)

        login = 'a-tomilin-22'
        year = 1994 + hash(f'{login}') % 23
        df = top_games.query('Year == @year')

        return df

    @task()
    def get_best_game(df):
        '''
        Какая игра была самой продаваемой в этом году во всем мире?
        '''
        best_game = df.query('Rank == Rank.max()').Name.values[0]
        return best_game


    @task()
    def get_best_genre(df):
        '''
        Игры какого жанра были самыми продаваемыми в Европе?
        Перечислить все, если их несколько
        '''
        game_eur = df.groupby('Genre', as_index= False)\
                .agg({'EU_Sales':'sum'})\
                .rename(columns = {'EU_Sales':'Total_sales'})\
                .sort_values('Total_sales', ascending = False)

        best_genre = game_eur.query('Total_sales == Total_sales.max()').Genre.values[0]
        return best_genre


    @task()
    def get_best_platform(df):
        '''
        На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        Перечислить все, если их несколько

        В самих данных Sales - это и есть тираж. То есть там не деньги, а миллионы копий.
        '''

        df_million_games = df.query('NA_Sales > 1.0')\
                            .groupby('Platform', as_index = False)\
                            .agg({'Name':'count'})\
                            .rename(columns = {'Name':'count_of_games'})\
                            .sort_values('count_of_games', ascending = False)

        best_platform = df_million_games.query('count_of_games == count_of_games.max()').Platform.values[0]
        return best_platform


    @task()
    def get_best_publisher(df):
        '''
        У какого издателя самые высокие средние продажи в Японии?
        Перечислить все, если их несколько
        '''
        df_jup_publish = df.groupby('Publisher', as_index = False)\
                            .agg({'JP_Sales':'mean'})\
                            .rename(columns = {'JP_Sales':'mean_JP_Sales'})\
                            .sort_values('mean_JP_Sales', ascending = False)

        best_publisher = df_jup_publish.query('mean_JP_Sales == mean_JP_Sales.max()').Publisher.values[0]
        return best_publisher


    @task()
    def get_Eur_game_count(df):
        '''
        Сколько игр продались лучше в Европе, чем в Японии?
        '''
        Eur_game_count = df.query('EU_Sales > JP_Sales').Name.count()
        return Eur_game_count


    @task(on_success_callback=send_message)
    def print_data(best_game, best_genre, best_platform, best_publisher, Eur_game_count):
        context = get_current_context()
        date = context['ds']

        print(f'''Data for {date}
                  Best game: {best_game}
                  Best genre: {best_genre}
                  Best platform: {best_platform}
                  Best publisher: {best_publisher}
                  Games in Europ: {Eur_game_count}
                  ''')


    df = get_data()
    best_game = get_best_game(df)
    best_genre = get_best_genre(df)
    best_platform = get_best_platform(df)
    best_publisher = get_best_publisher(df)
    Eur_game_count = get_Eur_game_count(df)

    print_data(best_game, best_genre, best_platform, best_publisher, Eur_game_count)

a_tomilin_game_stat = a_tomilin_game_stat()





