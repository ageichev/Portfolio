import requests
import pandas as pd
import numpy as np
import telegram

from datetime import timedelta
from datetime import datetime
from io import StringIO


from airflow.decorators import dag,task
from airflow.operators.python import get_current_context
from airflow.models import Variable

#определяем исследуемый год
year_numb = 1994 + hash(f'd-korzinin-18') % 23

default_args = {
    'owner': 'd-korzinin-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 16),
}


CHAT_ID = -683590609
BOT_TOKEN = Variable.get('telegram_secret')

# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} complited at {date}'
    bot = telegram.Bot(token = BOT_TOKEN)
    bot.send_message(chat_id = CHAT_ID, text = message)
    
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token = BOT_TOKEN)
#         bot.send_message(chat_id = CHAT_ID, text = message)
#     else:
#         pass

    
@dag(default_args=default_args, catchup = False)

def games_airfl2_d_korzinin_18():
    @task()
#   выгрузка данных
    def get_data():
        df = pd.read_csv('vgsales.csv')
        games = df.query('Year == @year_numb')
        return games
    
    @task()
#   Какая игра была самой продаваемой в этом году во всем мире?
    def best_sale(games):
        games_df2 = games.groupby('Name').sum().Global_Sales.idxmax()
        return games_df2
        

    @task()
#   Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def best_genre_eu(games):
        games_df3 = games.groupby('Genre').sum().EU_Sales.idxmax()
        return games_df3
    
    @task()
#   На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
#   Перечислить все, если их несколько
    def best_platform_na(games):
        games_df4 = games.query('NA_Sales > 1').groupby('Platform').sum().NA_Sales.idxmax()
        return games_df4
    
    @task()
#   У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    def best_publish_jp(games):
        games_df5 = games.groupby('Publisher').sum().JP_Sales.idxmax()
        return games_df5
    
    @task()
#   Сколько игр продались лучше в Европе, чем в Японии?
    def eu_vs_jp(games):
        games_df6 = games.groupby('Name', as_index = False) \
            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
            .query('EU_Sales - JP_Sales > 0').shape[0]
        return games_df6
    
    @task(on_success_callback = send_message)
#   вывод результатов
    def print_data(games_df2, games_df3, games_df4, games_df5, games_df6):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'В {year_numb} году лучше всего продавалась игра {games_df2}')
        print(f'В {year_numb} в Европе лучше всего продавались игры в жанре {games_df3}')
        print(f'В {year_numb} в Северной Америке более чем милионным тиражом больше всего продавались игры на платформе {games_df4}')
        print(f'В {year_numb} в Японии самые высокие средние продажи были у издателя {games_df5}')
        print(f'В {year_numb} в Европе {games_df6} игр продавались лучше чем в Японии')
        

    games = get_data()
    games_df2 = best_sale(games)
    games_df3 = best_genre_eu(games)
    games_df4 = best_platform_na(games)
    games_df5 = best_publish_jp(games)
    games_df6 = eu_vs_jp(games)
    print_data(games_df2, games_df3, games_df4, games_df5, games_df6)
    
games_airfl2_d_korzinin_18 = games_airfl2_d_korzinin_18()

