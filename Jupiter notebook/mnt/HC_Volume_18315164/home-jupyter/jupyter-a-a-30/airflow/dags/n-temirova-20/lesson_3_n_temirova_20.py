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
    'owner': 'n-temirova-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 6, 28),
    'schedule_interval': '0 10 * * *'
}

CHAT_ID = 363755680
BOT_TOKEN = Variable.get('telegram_secret')

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f"DAG {dag_id} successfully completed on {date}."
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False)
def lesson_3_n_temirova_20():
    
    @task(retries=3)
    def get_data():
        year = 1994 + hash(f'{"n-temirova-20"}') % 23
        games = pd.read_csv("vgsales.csv").query("Year == @year")
        return games
    
# 1.Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3)
    def best_selling_game(games):
        best_selling_game_df = games.groupby('Name', as_index=False)                                .agg({'Global_Sales': 'sum'})                                .sort_values('Global_Sales', ascending=False)                                .head(1).Name
        return best_selling_game_df

# 2.Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько.
    @task(retries=3)
    def best_selling_genre(games):
        best_selling_genre_df = games.groupby('Genre', as_index=False)                                 .agg({'EU_Sales': 'sum'})                                 .sort_values('EU_Sales', ascending=False)                                 .head(1).Genre
        return best_selling_genre_df

# 3.На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько.
    @task(retries=3)
    def NA_top_platform(games):
        NA_top_platform_df = games.query('NA_Sales > 1')                              .groupby('Platform', as_index = False)                              .agg({'Name': pd.Series.nunique})                              .sort_values('Name', ascending=False)                              .head(1).Platform
        return NA_top_platform_df

# 4.У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько.
    @task(retries=3)
    def JP_top_publisher(games):
        JP_top_publisher_df = games.groupby('Publisher', as_index = False)                               .agg({'JP_Sales': 'mean'})                               .sort_values('JP_Sales', ascending=False)                               .head(1).Publisher
        return JP_top_publisher_df

# 5.Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3)
    def EU_more_than_JP(games):
        EU_more_than_JP_df = games.groupby('Name', as_index = False)                                  .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})                                  .query('EU_Sales > JP_Sales')                                  .shape[0]
        return EU_more_than_JP_df

    @task(on_success_callback=send_message)
    def print_data(best_selling_game_df, best_selling_genre_df, NA_top_platform_df, JP_top_publisher_df, EU_more_than_JP_df):
        context = get_current_context()
        date = context['ds']
        year = 1994 + hash('n-temirova-20') % 23
        print(f"The most popular game in the world in {year} is {best_selling_game_df}")
        print(f"The most popular game genres in Europe in {year} is {best_selling_genre_df}")
        print(f"The most popular platform with games which have sold more than 1 million copies in {year} in North America is {NA_top_platform_df}")
        print(f"The best publisher with mean sales in Japan in {year} is {JP_top_publisher_df}")
        print(f"{EU_more_than_JP_df} games in {year} have sold more in Europe than in Japan")
    
    data = get_data()
    top_game = best_selling_game(data)
    top_genres = best_selling_genre(data)
    top_platform_na = NA_top_platform(data)
    top_publisher_jp = JP_top_publisher(data)
    EU_more_than_JP = EU_more_than_JP(data)
    print_data(top_game, top_genres, top_platform_na, top_publisher_jp, EU_more_than_JP)
    
lesson_3_n_temirova_20 = lesson_3_n_temirova_20()

