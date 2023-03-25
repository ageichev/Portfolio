import requests
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

import telegram

default_args = {
    'owner': 'k-shikalova-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 19),
    'schedule_interval': '23 50 * * *'
}

CHAT_ID = -785419568
BOT_TOKEN = Variable.get('telegram_secret')

# определим год, за который будем брать данные
year = 1994 + hash('k-shikalova-20') % 23

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! New dag {dag_id} completed on {date}. Oh yeah!!!'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False) #декоратор для дага
def k_shikalova_3_lesson_hw():

    @task()
    def get_data(): 

        # забираем нужные данные
        # path = '/mnt/HC_Volume_18315164/home-jupyter/jupyter-k-shikalova-20/Airflow/vgsales.csv'
        path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        vgsales = pd.read_csv(path).query('Year == @year')
        
        return vgsales.to_csv(index=False)

    @task()
    # Какая игра была самой продаваемой в этом году во всем мире?
    def top_game(vgsales):
        
        vgsales = pd.read_csv(StringIO(vgsales))
        game = vgsales \
            .groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .head(1) \
            .iloc[0]['Name']

        return game

    @task()  
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def eu_genres(vgsales):
        vgsales = pd.read_csv(StringIO(vgsales))
        genres_sales = vgsales \
                        .groupby('Genre', as_index=False) \
                        .agg({'EU_Sales': 'sum'})
        max_genre_sales = genres_sales.EU_Sales.max()
        genres = genres_sales \
                    .query('EU_Sales == @max_genre_sales') \
                    .Genre \
                    .tolist()
        
        genres = ', '.join(genres)

        return genres
    
    @task()
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    def popular_platform(vgsales):
        vgsales = pd.read_csv(StringIO(vgsales))
        na_sales = vgsales \
                    .groupby(['Platform', 'Name'], as_index=False) \
                    .agg({'NA_Sales': 'sum'}) \
                    .query('NA_Sales > 1') \
                    .groupby('Platform', as_index=False) \
                    .agg({'Name': 'count'})
        max_games_count = na_sales.Name.max()
        popular_platform = na_sales.query('Name == @max_games_count').Platform.tolist()
        popular_platform = ', '.join(popular_platform)

        return popular_platform
    
    @task()
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    def jap_publisher(vgsales):
        vgsales = pd.read_csv(StringIO(vgsales))
        publisher_sales = vgsales \
                            .groupby('Publisher', as_index=False) \
                            .agg({'JP_Sales': 'mean'})
        publisher_sales_max = publisher_sales.JP_Sales.max()
        jap_publisher = publisher_sales.query('JP_Sales == @publisher_sales_max').Publisher.tolist()
        jap_publisher = ', '.join(jap_publisher)

        return jap_publisher  
    
    @task()
    # Сколько игр продались лучше в Европе, чем в Японии?
    def eu_jap(vgsales):
        vgsales = pd.read_csv(StringIO(vgsales))
        n_games = vgsales \
                    .groupby('Name', as_index=False) \
                    .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
                    .query('EU_Sales > JP_Sales') \
                    .shape[0]

        return n_games    

    @task(on_success_callback=send_message)
    def print_data(genres, popular_platform, jap_publisher, n_games, game):

        context = get_current_context()
        
        # Какая игра была самой продаваемой в этом году во всем мире?
        print(f'The best-selling game in {year} was {game}')
        
        # Игры какого жанра были самыми продаваемыми в Европе?
        print(f'The most popular genre(s) in EU in {year} was(were) {genres}')
        
        # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
        print(f'The platform(s) with the biggest qty of high edition games in NA in {year} was (were) {popular_platform}')
        
        # У какого издателя самые высокие средние продажи в Японии? 
        print(f'The Publisher(s) with the highest average sales in Japan in {year} was (were) {jap_publisher}')
        
        # Сколько игр продались лучше в Европе, чем в Японии?
        print(f'The number of games which sales in {year} in EU were higher than in JP was {n_games}')

    vgsales = get_data()
    game = top_game(vgsales)
    genres = eu_genres(vgsales)
    popular_platform = popular_platform(vgsales)
    jap_publisher = jap_publisher(vgsales)
    n_games = eu_jap(vgsales)
    print_data(genres, popular_platform, jap_publisher, n_games, game)

k_shikalova_3_lesson_hw = k_shikalova_3_lesson_hw()